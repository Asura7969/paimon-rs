use std::sync::Arc;

use datafusion::{
    execution::{
        context::SessionState,
        runtime_env::{RuntimeConfig, RuntimeEnv},
    },
    prelude::{SessionConfig, SessionContext},
};

use self::paimon_datafusion::PaimonTableFactory;

mod builder;
mod dialect;
pub mod paimon;
mod paimon_datafusion;

pub fn context_with_delta_table_factory() -> SessionContext {
    let cfg = RuntimeConfig::new();
    let env = RuntimeEnv::new(cfg).unwrap();
    let ses = SessionConfig::new();
    let mut state = SessionState::with_config_rt(ses, Arc::new(env));
    state
        .table_factories_mut()
        .insert("PAIMON".to_string(), Arc::new(PaimonTableFactory {}));
    SessionContext::with_state(state)
}
