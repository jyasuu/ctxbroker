//! UNVERIFIED: written against the commonly-documented `#[tool_router]` /
//! `#[tool_handler]` macro pattern used in rmcp guides. The search I ran
//! flagged that rmcp 3.0.x (MSRV 1.88) changed protocol details (sessionless
//! HTTP, no `initialize`) -- I don't know if it also renamed these macros or
//! the `Parameters<T>` wrapper. Pin an exact rmcp version in Cargo.toml
//! (`rmcp = "=2.2.0"` is the last pre-3.0 release per the search results, and
//! probably the safer target to start from) and run `cargo doc --open -p rmcp`
//! to confirm this shape before relying on it.

use crate::broker::MessageBroker;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::tool::Parameters;
use rmcp::model::{Implementation, ServerCapabilities, ServerInfo};
use rmcp::{tool, tool_handler, tool_router, ServerHandler, ServiceExt};
use schemars::JsonSchema;
use serde::Deserialize;
use std::future::Future;
use std::sync::Arc;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SendMessageParams {
    /// Destination topic/queue name.
    pub topic: String,
    /// Message body. Left as a plain string -- callers that want structured
    /// data should JSON-encode it themselves; we don't assume a schema here.
    pub body: String,
}

#[derive(Clone)]
pub struct CtxBrokerMcp {
    broker: Arc<dyn MessageBroker>,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl CtxBrokerMcp {
    pub fn new(broker: Arc<dyn MessageBroker>) -> Self {
        Self {
            broker,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(description = "Publish a message onto the broker for other agents/sessions to fetch")]
    async fn send_message(&self, Parameters(params): Parameters<SendMessageParams>) -> String {
        // Same publish path the `ctxbroker send` CLI subcommand uses -- see
        // publish_message() in main.rs. Keeping exactly one publish code path
        // means the MCP tool and the CLI can never drift on delivery-mode,
        // topic handling, etc.
        match self.broker.publish(&params.topic, params.body.as_bytes()).await {
            Ok(()) => serde_json::json!({ "status": "published", "topic": params.topic }).to_string(),
            Err(e) => serde_json::json!({ "error": e.to_string() }).to_string(),
        }
    }
}

#[tool_handler]
impl ServerHandler for CtxBrokerMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            server_info: Implementation {
                name: "ctxbroker".into(),
                version: env!("CARGO_PKG_VERSION").into(),
            },
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            ..Default::default()
        }
    }
}

/// Runs the MCP server over stdio until the client disconnects.
pub async fn serve_stdio(broker: Arc<dyn MessageBroker>) -> anyhow::Result<()> {
    let service = CtxBrokerMcp::new(broker)
        .serve(rmcp::transport::stdio())
        .await?;
    service.waiting().await?;
    Ok(())
}
