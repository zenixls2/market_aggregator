mod orderbook;
use futures_util::{ready, task::Context, task::Poll, Stream};
pub use orderbook::orderbook_aggregator_client::*;
pub use orderbook::orderbook_aggregator_server::*;
pub use orderbook::{Empty, Level, Summary};
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::task::JoinHandle;
use tokio_util::sync::ReusableBoxFuture;

use std::pin::Pin;
use tonic::{Code, Request, Response, Status};

// A wrapper on the grpc server api
#[derive(Debug)]
pub struct AggServer {
    pub tx: UnboundedSender<Result<Summary, Status>>,
    #[allow(dead_code)]
    main_loop: JoinHandle<()>, // To have same lifetime as AggServer
    #[allow(dead_code)]
    broadcast_rx: broadcast::Receiver<Result<Summary, Status>>, // To have same lifetimea s AggServer
    broadcast_tx: broadcast::Sender<Result<Summary, Status>>,
}

impl AggServer {
    pub fn new() -> AggServer {
        let (tx, mut rx) = unbounded_channel();
        let (btx, brx) = broadcast::channel(20);
        let cbtx = btx.clone();
        let handle = tokio::spawn(async move {
            while let Some(item) = rx.recv().await {
                cbtx.send(item).unwrap();
            }
        });
        AggServer {
            main_loop: handle,
            tx,
            broadcast_rx: brx,
            broadcast_tx: btx,
        }
    }
}

type SummaryResult = Result<Summary, Status>;

pub struct BroadcastStream {
    inner: ReusableBoxFuture<'static, (SummaryResult, broadcast::Receiver<SummaryResult>)>,
}

async fn make_future(
    mut rx: broadcast::Receiver<SummaryResult>,
) -> (
    Result<Summary, Status>,
    broadcast::Receiver<Result<Summary, Status>>,
) {
    let result = rx.recv().await.unwrap_or_else(|e| match e {
        RecvError::Closed => Err(Status::new(Code::Aborted, "closed")),
        RecvError::Lagged(_) => Err(Status::new(Code::DeadlineExceeded, "timeout")),
    });
    (result, rx)
}

impl BroadcastStream {
    pub fn new(rx: broadcast::Receiver<SummaryResult>) -> Self {
        Self {
            inner: ReusableBoxFuture::new(make_future(rx)),
        }
    }
}

impl Stream for BroadcastStream {
    type Item = Result<Summary, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (result, rx) = ready!(self.inner.poll(cx));
        self.inner.set(make_future(rx));
        match result {
            Ok(item) => Poll::Ready(Some(Ok(item))),
            Err(status) => match status.code() {
                Code::Aborted => Poll::Ready(None),
                _ => Poll::Ready(Some(Err(status))),
            },
        }
    }
}

#[tonic::async_trait]
impl OrderbookAggregator for AggServer {
    type BookSummaryStream = BroadcastStream;
    async fn book_summary(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::BookSummaryStream>, Status> {
        let btx = self.broadcast_tx.clone();
        let brx = btx.subscribe();

        Ok(Response::new(BroadcastStream::new(brx)))
    }
}
