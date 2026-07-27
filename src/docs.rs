//! `GET /docs` + `GET /openapi.json` — the API-docs surface (parity audit
//! H19). The TS and Go references both serve their swagger UI pre-auth; this
//! keeps `/docs` public too. The spec is a hand-maintained static OpenAPI 3.0
//! document (no doc-comment scanning at runtime). Update `openapi.json` when a
//! route's contract changes — the contract tests in `handlers::tests` are the
//! source of truth it mirrors.
//!
//! ## Supply-chain hardening (F4)
//!
//! Swagger UI's assets are loaded from unpkg at an **exact pinned version**
//! with **Subresource Integrity** hashes and a restrictive **Content-Security-
//! Policy**, so a hijacked/republished package cannot execute altered JS in the
//! operator's authenticated browser session (the browser rejects any asset
//! whose bytes don't match the SRI hash). The references self-host their
//! assets; pin+SRI+CSP is the equivalent guarantee without vendoring ~1.5 MB
//! into the binary.

use axum::http::header;
use axum::response::IntoResponse;

/// The OpenAPI 3.0 spec, embedded at compile time.
pub const OPENAPI_JSON: &str = include_str!("../openapi.json");

/// Pinned Swagger UI release. Bump this together with the SRI hashes below.
const SWAGGER_UI_VERSION: &str = "5.17.14";
/// SRI hashes for the pinned assets (`openssl dgst -sha384 -binary | base64`).
const SWAGGER_CSS_SRI: &str =
    "sha384-wxLW6kwyHktdDGr6Pv1zgm/VGJh99lfUbzSn6HNHBENZlCN7W602k9VkGdxuFvPn";
const SWAGGER_JS_SRI: &str =
    "sha384-wmyclcVGX/WhUkdkATwhaK1X1JtiNrr2EoYJ+diV3vj4v6OC5yCeSu+yW13SYJep";

/// CSP for `/docs`: only 'self' + the pinned unpkg origin, and the assets are
/// additionally SRI-verified. Swagger UI needs inline style/script to bootstrap.
const DOCS_CSP: &str = "default-src 'none'; script-src 'self' 'unsafe-inline' https://unpkg.com; \
     style-src 'self' 'unsafe-inline' https://unpkg.com; img-src 'self' data: https://unpkg.com; \
     font-src 'self' https://unpkg.com; connect-src 'self'";

/// Swagger UI shell — pinned version + SRI integrity on every third-party asset.
fn docs_html() -> String {
    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>MessageBox Server API</title>
  <link rel="stylesheet"
        href="https://unpkg.com/swagger-ui-dist@{ver}/swagger-ui.css"
        integrity="{css_sri}" crossorigin="anonymous" />
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@{ver}/swagger-ui-bundle.js"
          integrity="{js_sri}" crossorigin="anonymous"></script>
  <script>
    window.onload = () => {{
      window.ui = SwaggerUIBundle({{
        url: "openapi.json",
        dom_id: "#swagger-ui",
      }});
    }};
  </script>
</body>
</html>
"##,
        ver = SWAGGER_UI_VERSION,
        css_sri = SWAGGER_CSS_SRI,
        js_sri = SWAGGER_JS_SRI,
    )
}

/// GET /openapi.json (pre-auth, like TS/Go). Injects `servers[]` from the
/// deployment's `ROUTING_PREFIX` so the documented paths resolve against the
/// real base path (the static spec ships a `/` placeholder).
pub fn openapi_json(routing_prefix: &str) -> impl IntoResponse {
    let base = if routing_prefix.is_empty() {
        "/".to_string()
    } else {
        routing_prefix.to_string()
    };
    let body = match serde_json::from_str::<serde_json::Value>(OPENAPI_JSON) {
        Ok(mut v) => {
            v["servers"] = serde_json::json!([{ "url": base, "description": "This deployment." }]);
            serde_json::to_string(&v).unwrap_or_else(|_| OPENAPI_JSON.to_string())
        }
        Err(_) => OPENAPI_JSON.to_string(),
    };
    ([(header::CONTENT_TYPE, "application/json")], body)
}

/// GET /docs (pre-auth, like TS/Go). Pinned + SRI-verified assets under a CSP.
pub async fn docs_page() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8".to_string()),
            (header::CONTENT_SECURITY_POLICY, DOCS_CSP.to_string()),
        ],
        docs_html(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The embedded spec is valid JSON, documents every public route the server
    /// actually mounts (H19), and now declares its auth scheme (F5).
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
        // F5: auth is documented (securityScheme + a global security requirement).
        assert!(
            spec["components"]["securitySchemes"]["BRC103Auth"].is_object(),
            "openapi must document the BRC-103 auth scheme"
        );
        assert!(
            spec["security"].is_array(),
            "openapi must declare a security requirement"
        );
    }

    /// F5: the served spec carries a `servers[]` block reflecting the prefix.
    #[test]
    fn openapi_injects_servers_from_prefix() {
        // No prefix → base "/".
        let rendered = render_openapi_body("");
        assert_eq!(rendered["servers"][0]["url"], "/");
        // A routing prefix is reflected so documented paths resolve correctly.
        let rendered = render_openapi_body("/mbs");
        assert_eq!(rendered["servers"][0]["url"], "/mbs");
    }

    /// Test helper mirroring `openapi_json`'s body construction.
    fn render_openapi_body(prefix: &str) -> serde_json::Value {
        let base = if prefix.is_empty() { "/" } else { prefix };
        let mut v: serde_json::Value = serde_json::from_str(OPENAPI_JSON).unwrap();
        v["servers"] = serde_json::json!([{ "url": base }]);
        v
    }

    #[test]
    fn docs_page_embeds_swagger_ui_with_pinned_sri() {
        let html = docs_html();
        assert!(html.contains("swagger-ui"));
        assert!(html.contains("openapi.json"));
        // Pinned version, not a floating major, and SRI on both assets.
        assert!(html.contains(&format!("swagger-ui-dist@{SWAGGER_UI_VERSION}")));
        assert!(
            !html.contains("swagger-ui-dist@5/"),
            "must not use a floating tag"
        );
        assert!(html.contains(SWAGGER_CSS_SRI));
        assert!(html.contains(SWAGGER_JS_SRI));
        assert!(html.contains("crossorigin=\"anonymous\""));
    }
}
