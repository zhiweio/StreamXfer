use crate::config::{quote_ident, TableRef};
use crate::error::{Result, StreamXferError};
use crate::schema::mapping::SqlColumn;
use serde::{Deserialize, Serialize};
use tiberius::{AuthMethod, Config};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MssqlConnectionConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub trust_cert: bool,
}

impl MssqlConnectionConfig {
    pub fn from_url(raw: &str) -> Result<Self> {
        let normalized = raw.replace("mssql+pymssql:://", "mssql://");
        let rest = normalized.strip_prefix("mssql://").ok_or_else(|| {
            StreamXferError::InvalidConfig("mssql url must start with mssql scheme".into())
        })?;
        let (auth_host, database) = rest.split_once('/').ok_or_else(|| {
            StreamXferError::InvalidConfig("mssql url requires database path".into())
        })?;
        if database.is_empty() {
            return Err(StreamXferError::InvalidConfig(
                "mssql url requires database path".into(),
            ));
        }
        let (auth, host_port) = match auth_host.rsplit_once('@') {
            Some((auth, host_port)) => (Some(auth), host_port),
            None => (None, auth_host),
        };
        let (host, port) = match host_port.rsplit_once(':') {
            Some((host, port)) => (host.to_string(), port.parse().unwrap_or(1433)),
            None => (host_port.to_string(), 1433),
        };
        if host.is_empty() {
            return Err(StreamXferError::InvalidConfig(
                "mssql url requires host".into(),
            ));
        }
        let (username, password) = match auth.and_then(|auth| auth.split_once(':')) {
            Some((username, password)) => (Some(username.to_string()), Some(password.to_string())),
            None => (None, None),
        };
        Ok(Self {
            host,
            port,
            database: database.to_string(),
            username,
            password,
            trust_cert: true,
        })
    }

    pub fn tiberius_config(&self) -> Config {
        let mut config = Config::new();
        config.host(&self.host);
        config.port(self.port);
        config.database(&self.database);
        if let Some(username) = &self.username {
            config.authentication(AuthMethod::sql_server(
                username,
                self.password.as_deref().unwrap_or_default(),
            ));
        }
        if self.trust_cert {
            config.trust_cert();
        }
        config
    }
}

pub fn table_columns_sql(table: &TableRef) -> String {
    format!("select column_name, data_type, is_nullable, numeric_precision, numeric_scale from information_schema.columns where table_schema = N'{}' and table_name = N'{}' order by ordinal_position", table.schema.replace('\'', "''"), table.table.replace('\'', "''"))
}

pub fn table_select_sql(
    table: &TableRef,
    columns: &[SqlColumn],
    predicate: Option<&str>,
) -> String {
    let projection = if columns.is_empty() {
        "*".to_string()
    } else {
        columns
            .iter()
            .map(|col| quote_ident(&col.name))
            .collect::<Vec<_>>()
            .join(", ")
    };
    match predicate {
        Some(predicate) => format!(
            "select {projection} from {} where {predicate}",
            table.sql_name()
        ),
        None => format!("select {projection} from {}", table.sql_name()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn builds_tiberius_config_from_url() {
        let config = MssqlConnectionConfig::from_url("mssql://sa:pass@localhost:1433/db").unwrap();
        assert_eq!(config.host, "localhost");
        assert_eq!(config.database, "db");
    }
}
