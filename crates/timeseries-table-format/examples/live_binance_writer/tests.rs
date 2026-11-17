use super::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{
    io::{BufRead, BufReader},
    net::TcpListener,
    ops::Range,
    sync::{Mutex, atomic::AtomicUsize},
};
use tempfile::TempDir;

fn trade(id: u64) -> Trade {
    Trade {
        id,
        time: 1_700_000_000_000,
        price: "0.00000001".into(),
        quantity: "123456789.12345678".into(),
        buyer_is_maker: true,
    }
}

async fn table(root: &TempDir) -> Result<TimeSeriesTable> {
    Ok(TimeSeriesTable::create(
        TableLocation::local(root.path()),
        TableMeta::new_time_series_with_schema(
            IndexSpec {
                column: "trade_id".into(),
                entity_columns: vec![],
                kind: IndexKind::UInt64 {
                    index_granularity: NonZeroU64::MIN,
                },
            },
            LogicalSchema::try_from_arrow_schema(schema().as_ref())?,
        ),
    )
    .await?)
}

#[test]
fn trade_payloads_and_arrow_batch_preserve_decimal_strings() -> Result<()> {
    let rest = r#"{"a":17,"T":1700000000000,"p":"0.00000001","q":"123456789.12345678","m":true}"#;
    let event: Trade = serde_json::from_str(rest)?;
    assert_eq!(event, trade(17));
    let batch = batch(&[event])?;
    let prices = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("expected strings")?;
    let quantities = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("expected strings")?;
    assert_eq!(prices.value(0), "0.00000001");
    assert_eq!(quantities.value(0), "123456789.12345678");
    assert!(serde_json::from_str::<Trade>(r#"{"a":-1,"T":0,"p":"1","q":"1","m":true}"#).is_err());
    Ok(())
}

#[test]
fn websocket_answers_ping_and_validates_market() -> Result<()> {
    for symbol in ["BTCUSDT", "ETHUSDT"] {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let server = thread::spawn(move || -> Result<()> {
            let stream = listener.accept()?.0;
            stream.set_read_timeout(Some(Duration::from_secs(5)))?;
            let mut socket = tungstenite::accept(stream)?;
            socket.send(Message::Ping(vec![1, 2, 3].into()))?;
            let mut message = serde_json::to_value(trade(17))?;
            message["e"] = "aggTrade".into();
            message["s"] = symbol.into();
            socket.send(Message::Text(message.to_string().into()))?;
            assert_eq!(socket.read()?, Message::Pong(vec![1, 2, 3].into()));
            Ok(())
        });
        let (mut socket, _) = tungstenite::connect(format!("ws://{address}"))?;
        if symbol == "BTCUSDT" {
            assert_eq!(read_trade(&mut socket)?, trade(17));
        } else {
            assert!(read_trade(&mut socket).is_err());
        }
        server
            .join()
            .map_err(|_| io::Error::other("test server panicked"))??;
    }
    Ok(())
}

#[tokio::test]
async fn recovery_checks_coverage_instead_of_only_the_maximum_id() -> Result<()> {
    let root = TempDir::new()?;
    let mut table = table(&root).await?;
    assert_eq!(resume_from(&table).await?, None);
    let mut writer = table.writer(schema())?;
    for start in [20, 0] {
        writer
            .write(&batch(&(start..start + 10).map(trade).collect::<Vec<_>>())?)
            .await?;
        writer.commit().await?;
    }
    writer.abort().await?;
    assert!(resume_from(&table).await.is_err());
    let mut writer = table.writer(schema())?;
    writer
        .write(&batch(&(10..20).map(trade).collect::<Vec<_>>())?)
        .await?;
    writer.commit().await?;
    writer.abort().await?;
    assert_eq!(resume_from(&table).await?, Some(30));
    Ok(())
}

#[tokio::test]
async fn restart_cursor_ignores_the_uncommitted_tail() -> Result<()> {
    let root = TempDir::new()?;
    let mut table = table(&root).await?;
    let mut writer = table.writer(schema())?;
    writer
        .write(&batch(&(10..20).map(trade).collect::<Vec<_>>())?)
        .await?;
    writer.commit().await?;
    writer
        .write(&batch(&(20..30).map(trade).collect::<Vec<_>>())?)
        .await?;
    drop(writer);
    let reopened = TimeSeriesTable::open(TableLocation::local(root.path())).await?;
    assert_eq!(resume_from(&reopened).await?, Some(20));
    Ok(())
}

#[derive(Clone)]
enum RestReply {
    Trades(Vec<Trade>),
    Status(u16),
    Malformed,
    Disconnect,
    Timeout,
}

struct MockMarket {
    source: MarketSource,
    stop: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<(u64, usize)>>>,
    connections: Arc<AtomicUsize>,
    servers: Vec<thread::JoinHandle<Result<()>>>,
}

fn accept(listener: &TcpListener, stop: &AtomicBool) -> Result<Option<TcpStream>> {
    while !stop.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(false)?;
                stream.set_read_timeout(Some(Duration::from_secs(2)))?;
                stream.set_write_timeout(Some(Duration::from_secs(2)))?;
                return Ok(Some(stream));
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(None)
}

impl MockMarket {
    fn new(
        sessions: Vec<Vec<u64>>,
        mut reply: impl FnMut(u64, usize) -> RestReply + Send + 'static,
    ) -> Result<Self> {
        assert!(!sessions.is_empty());
        let ws = TcpListener::bind("127.0.0.1:0")?;
        let http = TcpListener::bind("127.0.0.1:0")?;
        ws.set_nonblocking(true)?;
        http.set_nonblocking(true)?;
        let source = MarketSource {
            stream: format!("ws://{}/ws/btcusdt@aggTrade", ws.local_addr()?),
            rest: format!("http://{}/api/v3/aggTrades", http.local_addr()?),
            timeout: Duration::from_millis(500),
        };
        let stop = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let connections = Arc::new(AtomicUsize::new(0));
        let ws_stop = Arc::clone(&stop);
        let ws_connections = Arc::clone(&connections);
        let ws_server = thread::spawn(move || -> Result<()> {
            while let Some(stream) = accept(&ws, &ws_stop)? {
                let index = ws_connections.fetch_add(1, Ordering::Relaxed);
                let mut socket = tungstenite::accept(stream)?;
                // Reconnects repeat the final session, including its stale messages.
                let ids = &sessions[index.min(sessions.len() - 1)];
                for &id in ids {
                    let mut value = serde_json::to_value(market_trade(id))?;
                    value["e"] = "aggTrade".into();
                    value["s"] = "BTCUSDT".into();
                    if socket
                        .send(Message::Text(value.to_string().into()))
                        .is_err()
                    {
                        break;
                    }
                }
                if index + 1 < sessions.len() {
                    socket.get_mut().shutdown(Shutdown::Both)?;
                    continue;
                }
                socket
                    .get_mut()
                    .set_read_timeout(Some(Duration::from_millis(20)))?;
                while !ws_stop.load(Ordering::Relaxed) {
                    match socket.read() {
                        Err(tungstenite::Error::Io(error))
                            if matches!(
                                error.kind(),
                                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                            ) => {}
                        Err(_) | Ok(Message::Close(_)) => break,
                        Ok(_) => {}
                    }
                }
            }
            Ok(())
        });
        let http_stop = Arc::clone(&stop);
        let http_requests = Arc::clone(&requests);
        let http_server = thread::spawn(move || -> Result<()> {
            while let Some(mut stream) = accept(&http, &http_stop)? {
                let mut reader = BufReader::new(&mut stream);
                let mut line = String::new();
                reader.read_line(&mut line)?;
                let target = line
                    .split_whitespace()
                    .nth(1)
                    .ok_or("missing HTTP target")?;
                let (path, query) = target.split_once('?').ok_or("missing HTTP query")?;
                assert_eq!(path, "/api/v3/aggTrades");
                let params: HashMap<_, _> =
                    query.split('&').filter_map(|p| p.split_once('=')).collect();
                assert_eq!(params.get("symbol"), Some(&"BTCUSDT"));
                let from: u64 = params.get("fromId").ok_or("missing fromId")?.parse()?;
                let limit: usize = params.get("limit").ok_or("missing limit")?.parse()?;
                assert!((1..=1000).contains(&limit));
                loop {
                    line.clear();
                    if reader.read_line(&mut line)? == 0 || line == "\r\n" {
                        break;
                    }
                }
                drop(reader);
                http_requests
                    .lock()
                    .map_err(|_| "requests poisoned")?
                    .push((from, limit));
                let (status, body) = match reply(from, limit) {
                    RestReply::Trades(trades) => (200, serde_json::to_string(&trades)?),
                    RestReply::Status(status) => (status, "{}".into()),
                    RestReply::Malformed => (200, "{".into()),
                    RestReply::Disconnect => continue,
                    RestReply::Timeout => {
                        thread::sleep(Duration::from_millis(200));
                        continue;
                    }
                };
                write!(
                    stream,
                    "HTTP/1.1 {status} Mock\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )?;
            }
            Ok(())
        });
        Ok(Self {
            source,
            stop,
            requests,
            connections,
            servers: vec![ws_server, http_server],
        })
    }

    fn requests(&self) -> Result<Vec<(u64, usize)>> {
        Ok(self
            .requests
            .lock()
            .map_err(|_| "requests poisoned")?
            .clone())
    }

    fn finish(&mut self) -> Result<()> {
        self.stop.store(true, Ordering::Relaxed);
        for server in self.servers.drain(..) {
            server.join().map_err(|_| "mock market panicked")??;
        }
        Ok(())
    }
}

impl Drop for MockMarket {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

fn market_trade(id: u64) -> Trade {
    Trade {
        id,
        time: 1_700_000_000_000 + id as i64,
        price: format!("{id}.00000001"),
        quantity: format!("{id}.12345678"),
        buyer_is_maker: id.is_multiple_of(2),
    }
}

fn history(from: u64, limit: usize) -> RestReply {
    RestReply::Trades((from..from + limit as u64).map(market_trade).collect())
}

fn options(root: &TempDir, rows: usize, commits: usize) -> Options {
    Options {
        root: root.path().into(),
        rows,
        commits,
        queue: 8,
        delay_ms: 0,
        disconnect_after: 0,
    }
}

async fn seed(root: &TempDir) -> Result<()> {
    let mut table = table(root).await?;
    let mut writer = table.writer(schema())?;
    writer
        .write(&batch(&(0..10).map(market_trade).collect::<Vec<_>>())?)
        .await?;
    writer.commit().await?;
    writer.abort().await?;
    Ok(())
}

async fn assert_committed(root: &TempDir, expected: Range<u64>, version: u64) -> Result<()> {
    let table = TimeSeriesTable::open(TableLocation::local(root.path())).await?;
    assert_eq!(table.state().version, version);
    let mut actual = Vec::new();
    for segment in table.state().segments.values() {
        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(
            root.path().join(&segment.path),
        )?)?
        .build()?;
        let mut segment_rows = 0;
        for record in reader {
            let record = record?;
            assert!(
                record
                    .columns()
                    .iter()
                    .all(|column| column.null_count() == 0)
            );
            let ids = record
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or("trade IDs")?;
            let times = record
                .column(1)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or("trade times")?;
            let prices = record
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("prices")?;
            let quantities = record
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("quantities")?;
            let makers = record
                .column(4)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("makers")?;
            for i in 0..record.num_rows() {
                actual.push(Trade {
                    id: ids.value(i),
                    time: times.value(i),
                    price: prices.value(i).into(),
                    quantity: quantities.value(i).into(),
                    buyer_is_maker: makers.value(i),
                });
            }
            segment_rows += record.num_rows() as u64;
        }
        assert_eq!(segment_rows, segment.row_count);
    }
    actual.sort_by_key(|trade| trade.id);
    assert_eq!(
        actual,
        expected.clone().map(market_trade).collect::<Vec<_>>()
    );
    assert_eq!(resume_from(&table).await?, Some(expected.end));
    Ok(())
}

#[tokio::test]
async fn duplicate_and_out_of_order_messages_overlap_rest_without_losing_rows() -> Result<()> {
    let root = TempDir::new()?;
    let ids = [100, 100, 102, 101, 102, 105, 104, 103, 105]
        .into_iter()
        .chain(106..200)
        .collect();
    let mut market = MockMarket::new(vec![ids], history)?;
    let result = run(options(&root, 4, 3), market.source.clone()).await;
    market.finish()?;
    let stats = result?;
    assert_eq!(stats.replayed_rows, 3);
    assert_eq!(market.requests()?, [(101, 1), (103, 2)]);
    assert_committed(&root, 100..112, 4).await
}

#[tokio::test]
async fn paginated_replay_resumes_after_a_midway_disconnect() -> Result<()> {
    let root = TempDir::new()?;
    seed(&root).await?;
    let mut disconnected = false;
    let mut market = MockMarket::new(
        vec![vec![2515], (2515..2600).collect()],
        move |from, limit| {
            if from == 1010 && !disconnected {
                disconnected = true;
                RestReply::Disconnect
            } else {
                history(from, limit)
            }
        },
    )?;
    let result = run(options(&root, 502, 5), market.source.clone()).await;
    market.finish()?;
    let stats = result?;
    assert_eq!(stats.reconnects, 1);
    assert_eq!(stats.replayed_rows, 2505);
    assert_eq!(
        market.requests()?,
        [(10, 1000), (1010, 1000), (1010, 1000), (2010, 505)]
    );
    assert_committed(&root, 0..2520, 7).await
}

#[tokio::test]
async fn temporarily_empty_history_catches_up_without_skipping_rows() -> Result<()> {
    let root = TempDir::new()?;
    seed(&root).await?;
    let mut empty = 2;
    let ids = (10..20).chain(25..80).collect();
    let mut market = MockMarket::new(vec![ids], move |from, limit| {
        if empty > 0 {
            empty -= 1;
            RestReply::Trades(vec![])
        } else {
            history(from, limit)
        }
    })?;
    let result = run(options(&root, 20, 1), market.source.clone()).await;
    market.finish()?;
    let stats = result?;
    assert_eq!(stats.reconnects, 0);
    assert_eq!(stats.replayed_rows, 5);
    assert_eq!(market.requests()?, [(20, 5); 3]);
    assert_committed(&root, 0..30, 3).await
}

#[tokio::test]
async fn transient_source_errors_reconnect_without_losing_pending_rows() -> Result<()> {
    for fault in [
        RestReply::Status(429),
        RestReply::Status(503),
        RestReply::Timeout,
        RestReply::Malformed,
    ] {
        let root = TempDir::new()?;
        seed(&root).await?;
        let timeout = matches!(fault, RestReply::Timeout);
        let mut first = Some(fault);
        let ids = (10..20).chain(25..80).collect();
        let mut market = MockMarket::new(vec![ids], move |from, limit| {
            first.take().unwrap_or_else(|| history(from, limit))
        })?;
        if timeout {
            market.source.timeout = Duration::from_millis(80);
        }
        let result = run(options(&root, 20, 1), market.source.clone()).await;
        market.finish()?;
        let stats = result?;
        assert_eq!(stats.reconnects, 1);
        assert_eq!(stats.replayed_rows, 5);
        assert_eq!(market.requests()?, [(20, 5); 2]);
        assert_committed(&root, 0..30, 3).await?;
    }
    Ok(())
}

#[tokio::test]
async fn permanent_source_errors_stop_without_publishing_a_partial_commit() -> Result<()> {
    let cases = [
        ("rate limit", RestReply::Status(429), "429"),
        ("server error", RestReply::Status(503), "503"),
        ("timeout", RestReply::Timeout, "timeout"),
        ("invalid JSON", RestReply::Malformed, "json:"),
        (
            "history gap",
            RestReply::Trades(vec![market_trade(21)]),
            "historical API gap",
        ),
        (
            "history duplicate",
            RestReply::Trades(vec![market_trade(20), market_trade(20)]),
            "historical API gap",
        ),
        (
            "history out of order",
            RestReply::Trades(vec![market_trade(20), market_trade(22), market_trade(21)]),
            "historical API gap",
        ),
        (
            "history never catches up",
            RestReply::Trades(vec![]),
            "historical API did not catch up",
        ),
    ];
    for (name, fault, reason) in cases {
        let root = TempDir::new()?;
        seed(&root).await?;
        let before = TimeSeriesTable::open(TableLocation::local(root.path()))
            .await?
            .state()
            .clone();
        let ids = (10..20).chain([25]).collect();
        let timeout = matches!(fault, RestReply::Timeout);
        let mut market = MockMarket::new(vec![ids], move |_, _| fault.clone())?;
        if timeout {
            market.source.timeout = Duration::from_millis(80);
        }
        let result = run(options(&root, 20, 1), market.source.clone()).await;
        market.finish()?;
        let error = result.err().ok_or_else(|| format!("{name} must fail"))?;
        assert!(error.to_string().contains(reason), "{name}: {error}");
        assert_eq!(
            market.connections.load(Ordering::Relaxed),
            6,
            "{name}: bounded retries"
        );
        let expected_requests = if name == "history never catches up" {
            60
        } else {
            6
        };
        assert_eq!(market.requests()?.len(), expected_requests, "{name}");
        let reopened = TimeSeriesTable::open(TableLocation::local(root.path())).await?;
        assert_eq!(reopened.state(), &before, "{name}: committed state changed");
        assert_committed(&root, 0..10, 2).await?;

        let mut recovered = MockMarket::new(vec![(35..100).collect()], history)?;
        let result = run(options(&root, 30, 1), recovered.source.clone()).await;
        recovered.finish()?;
        assert_eq!(result?.replayed_rows, 25);
        assert_eq!(recovered.requests()?, [(10, 25)]);
        assert_committed(&root, 0..40, 3).await?;
    }
    Ok(())
}

#[tokio::test]
async fn stalled_websocket_times_out_without_publishing_pending_rows() -> Result<()> {
    let root = TempDir::new()?;
    seed(&root).await?;
    let mut market = MockMarket::new(vec![(10..20).collect()], history)?;
    market.source.timeout = Duration::from_millis(200);
    let result = run(options(&root, 20, 1), market.source.clone()).await;
    market.finish()?;
    assert!(result.is_err());
    assert_eq!(market.connections.load(Ordering::Relaxed), 6);
    assert!(market.requests()?.is_empty());
    assert_committed(&root, 0..10, 2).await
}

#[tokio::test]
async fn sustained_backpressure_survives_repeated_disconnects_and_replay() -> Result<()> {
    let root = TempDir::new()?;
    let mut market = MockMarket::new(
        vec![
            (0..200).collect(),
            (400..600).collect(),
            (800..1200).collect(),
        ],
        history,
    )?;
    let mut options = options(&root, 100, 10);
    options.queue = 2;
    options.delay_ms = 10;
    let result = run(options, market.source.clone()).await;
    market.finish()?;
    let stats = result?;
    assert_eq!(stats.reconnects, 2);
    assert_eq!(stats.replayed_rows, 400);
    assert!(stats.backpressure_events > 2);
    assert!(stats.blocked_ms > 0);
    assert_eq!(market.requests()?, [(200, 200), (600, 200)]);
    assert_committed(&root, 0..1000, 11).await
}
