use std::any::type_name;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use temporal_sdk::WfContext;
use temporal_sdk::{sdk_client_options, Worker};
use temporal_sdk::{ActivityOptions, WfExitValue};
use temporal_sdk_core::protos::temporal::api::common::v1::Payload;
use temporal_sdk_core::{init_worker, CoreRuntime, Url};
use temporal_sdk_core_api::{telemetry::TelemetryOptionsBuilder, worker::WorkerConfigBuilder};
use temporal_sdk_core_protos::coresdk::activity_result::activity_resolution::Status;
use temporal_sdk_core_protos::coresdk::activity_result::{
    ActivityResolution, Cancellation, Failure, Success,
};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = "http://localhost:7233";
    let namespace = "default";

    let server_options_builder = sdk_client_options(Url::from_str(server)?);

    let server_options = server_options_builder.build()?;
    let client = server_options.connect(namespace, None, None).await?;
    let telemetry_options = TelemetryOptionsBuilder::default().build()?;
    let runtime = CoreRuntime::new_assume_tokio(telemetry_options)?;
    let worker_config = WorkerConfigBuilder::default()
        .namespace(namespace)
        .task_queue(QUEUE)
        .worker_build_id("djc-mbp")
        .build()?;

    let core_worker = init_worker(&runtime, worker_config, client)?;

    let mut worker = Worker::new_from_core(Arc::new(core_worker), QUEUE);
    State::new().register(&mut worker);
    worker.run().await?;

    Ok(())
}

struct State(());

impl State {
    fn new() -> Self {
        Self(())
    }

    fn register(self, worker: &mut Worker) {
        let state = Arc::new(self);

        let cloned = state.clone();
        worker.register_wf("provision", move |cx| {
            let state = cloned.clone();
            async move {
                let Input { key } = Input::from_argument(0, "provision", &cx)?;
                state.provision(key, cx).await
            }
        });

        let cloned = state.clone();
        worker.register_activity("preflight", move |_, input| {
            let state = cloned.clone();
            async move {
                let Input { key } = input;
                state.preflight(key).await
            }
        });

        let cloned = state.clone();
        worker.register_activity("setup", move |_, input| {
            let state = cloned.clone();
            async move {
                let Input { key } = input;
                state.setup(key).await
            }
        });

        let cloned = state.clone();
        worker.register_activity("finalize", move |_, input| {
            let state = cloned.clone();
            async move {
                let FinalizeInput { url, key } = input;
                state.finalize(url, key).await
            }
        });
    }

    async fn provision(&self, key: String, cx: WfContext) -> anyhow::Result<WfExitValue<()>> {
        // TODO: reduce boilerplate for kicking off activities
        let mut preflight = ActivityOptions::default();
        preflight.activity_id = Some(format!("preflight-{}", key));
        preflight.activity_type = "preflight".to_owned();
        preflight.task_queue = QUEUE.to_owned();
        // Must set either `schedule_to_close_timeout` or `start_to_close_timeout`
        preflight.schedule_to_close_timeout = Some(Duration::from_secs(15 * 60));
        // Must provide a payload that can deserialize as the payload type given in the
        preflight.input = Input { key: key.clone() }.to_payload()?;

        let resolution = cx.activity(preflight).await;
        <()>::extract(resolution, "preflight")?;

        let mut setup = ActivityOptions::default();
        setup.activity_id = Some(format!("setup-{}", key));
        setup.activity_type = "setup".to_owned();
        setup.task_queue = QUEUE.to_owned();
        // Must set either `schedule_to_close_timeout` or `start_to_close_timeout`
        setup.schedule_to_close_timeout = Some(Duration::from_secs(15 * 60));
        // Must provide a payload that can deserialize as the payload type given in the
        setup.input = Input { key: key.clone() }.to_payload()?;

        let resolution = cx.activity(setup).await;
        let order = Order::extract(resolution, "setup")?;

        let mut finalize = ActivityOptions::default();
        finalize.activity_id = Some(format!("finalize-{}", key));
        finalize.activity_type = "finalize".to_owned();
        finalize.task_queue = QUEUE.to_owned();
        // Must set either `schedule_to_close_timeout` or `start_to_close_timeout`
        finalize.schedule_to_close_timeout = Some(Duration::from_secs(15 * 60));
        // Must provide a payload that can deserialize as the payload type given in the
        finalize.input = FinalizeInput {
            url: order.url,
            key: key.clone(),
        }
        .to_payload()?;

        let resolution = cx.activity(finalize).await;
        <()>::extract(resolution, "finalize")?;

        self.cleanup(&key).await?;
        Ok(().into())
    }

    async fn preflight(&self, _key: String) -> anyhow::Result<()> {
        sleep(Duration::from_millis(100)).await;
        Ok(())
    }

    async fn setup(&self, _key: String) -> anyhow::Result<Order> {
        sleep(Duration::from_secs(4)).await;
        Ok(Order {
            url: "http://example.com".to_owned(),
        })
    }

    async fn finalize(&self, _url: String, _key: String) -> anyhow::Result<()> {
        sleep(Duration::from_secs(2)).await;
        Ok(())
    }

    async fn cleanup(&self, _key: &str) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Input {
    key: String,
}

impl WorkflowType for Input {}

#[derive(Debug, Deserialize, Serialize)]
struct Order {
    url: String,
}

impl WorkflowType for Order {}

#[derive(Debug, Deserialize, Serialize)]
struct FinalizeInput {
    url: String,
    key: String,
}

impl WorkflowType for FinalizeInput {}

trait WorkflowType: DeserializeOwned + Serialize {
    fn extract(value: ActivityResolution, source: &str) -> Result<Self, anyhow::Error> {
        let payload = match value.status {
            Some(Status::Completed(Success {
                result: Some(payload),
            })) => payload,
            Some(Status::Failed(Failure {
                failure: Some(failure),
            })) => return Err(anyhow::Error::msg(format!("{source} failed: {failure:?}"))),
            Some(Status::Cancelled(Cancellation {
                failure: Some(failure),
            })) => {
                return Err(anyhow::Error::msg(format!(
                    "{source} cancelled: {failure:?}"
                )))
            }
            status => {
                return Err(anyhow::Error::msg(format!(
                    "no status from {source} activity: {status:?}"
                )))
            }
        };

        Self::from_payload(&payload, source)
    }

    fn from_argument(idx: usize, source: &str, cx: &WfContext) -> Result<Self, anyhow::Error> {
        Self::from_payload(
            cx.get_args().get(idx).ok_or_else(|| {
                anyhow::Error::msg(format!(
                    "missing argument of type {:?} at index {idx}",
                    type_name::<Self>(),
                ))
            })?,
            source,
        )
    }

    fn from_payload(payload: &Payload, source: &str) -> Result<Self, anyhow::Error> {
        match payload.metadata.get("encoding").map(|s| s.as_slice()) {
            Some(ENCODING_JSON) => Ok(serde_json::from_slice(&payload.data)?),
            encoding => Err(anyhow::Error::msg(format!(
                "{source} returned unexpected encoding {encoding:?}"
            ))),
        }
    }

    fn to_payload(&self) -> Result<Payload, serde_json::Error> {
        let mut metadata = HashMap::default();
        metadata.insert("encoding".to_owned(), ENCODING_JSON.to_vec());

        Ok(Payload {
            metadata,
            data: serde_json::to_vec(self).unwrap(),
        })
    }
}

impl<T: WorkflowType> WorkflowType for Vec<T> {}
impl WorkflowType for () {}

pub const QUEUE: &str = "task_queue";

const ENCODING_JSON: &[u8] = b"json/plain";
