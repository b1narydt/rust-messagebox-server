//! `GET /docs` + `GET /openapi.json` — the API-docs surface (parity audit
//! H19). The TS server serves swagger-jsdoc output at the same two pre-auth
//! paths; here the spec is a hand-maintained static OpenAPI 3.0 document
//! (no doc-comment scanning at runtime). Update `openapi.json` when a
//! route's contract changes — the contract tests in `handlers::tests` are
//! the source of truth it mirrors.

use axum::http::header;
use axum::response::IntoResponse;

/// The OpenAPI 3.0 spec, embedded at compile time.
pub const OPENAPI_JSON: &str = include_str!("../openapi.json");

/// Swagger UI shell pointing at `/openapi.json`. The UI assets load from the
/// unpkg CDN in the operator's browser — the server itself makes no external
/// requests.
const DOCS_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>MessageBox Server API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = () => {
      window.ui = SwaggerUIBundle({
        url: "/openapi.json",
        dom_id: "#swagger-ui",
      });
    };
  </script>
</body>
</html>
"##;

/// GET /openapi.json (pre-auth, like TS).
pub async fn openapi_json() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "application/json")], OPENAPI_JSON)
}

/// GET /docs (pre-auth, like TS).
pub async fn docs_page() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        DOCS_HTML,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The embedded spec is valid JSON and documents every public route the
    /// server actually mounts (H19 — the TS docs surface, ported).
    #[test]
    fn openapi_spec_is_valid_and_covers_the_public_surface() {
        let spec: serde_json::Value =
            serde_json::from_str(OPENAPI_JSON).expect("openapi.json must parse");
        assert_eq!(spec["openapi"], "3.0.3");
        let paths = spec["paths"].as_object().expect("paths object");
        for route in [
            "/sendMessage",
            "/listMessages",
            "/acknowledgeMessage",
            "/registerDevice",
            "/devices",
            "/permissions/set",
            "/permissions/get",
            "/permissions/list",
            "/permissions/quote",
            "/docs",
            "/openapi.json",
        ] {
            assert!(paths.contains_key(route), "spec missing route {route}");
        }
    }

    #[test]
    fn docs_page_embeds_swagger_ui_over_the_spec() {
        assert!(DOCS_HTML.contains("swagger-ui"));
        assert!(DOCS_HTML.contains("/openapi.json"));
    }
}
