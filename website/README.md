# NetAudio website

The website is plain static HTML with one shared stylesheet and small, page-specific progressive enhancements where the document needs them. It has no application framework, template layer, frontend package manager, or generated page source.

`public/` is the complete public document root. Each page is a complete semantic HTML document. `public/assets/site.css` owns the shared visual system and the general product, documentation index, long-form document, and release-entry layout primitives. `public/assets/contents-navigation.js` progressively identifies the current section in long documents while preserving ordinary anchor navigation when scripting is unavailable.

The current public routes are `/`, `/support`, and `/privacy`. Add a navigation item only when its destination contains useful public content.

Record consequential design, content, or infrastructure decisions that were not explicitly specified in `DECISIONS.md` before publishing. Keep the entries chronological and state both the reasoning and what would change if the decision is revisited.

Run `make site-check` to validate the static source and its links, metadata, accessibility structure, legal-content safeguards, and assets. Run `make site-publish` to create and atomically activate a content-addressed release under `/srv/http/netaudio`.

Published releases are immutable directories under `/srv/http/netaudio/releases`. `/srv/http/netaudio/current` is an atomically replaced symlink to the active release. To roll back, run `sudo /usr/bin/python3 website/publish.py --activate RELEASE_IDENTIFIER` with an existing release identifier.

The Caddy directives in `caddy/netaudio.caddy` are installed at `/etc/caddy/netaudio.caddy` and imported from the shared system Caddyfile. Static releases do not require a Caddy reload. Validate and reload Caddy whenever the tracked Caddy directives change.

All current pages use the same shared page width for the header, main content, and footer. General product pages use `.product-introduction`, `.product-status`, `.content-section`, `.capability-list`, and `.resource-list`. Support, documentation, and legal pages use `.content-page` as their main grouping. Use `.page-contents` only when a document is long enough to benefit from in-page navigation. Support indexes use `.documentation-index` and `.documentation-section`; long-form policies and technical documents use `.document-header`, `.document-introduction`, and `.legal-document`. Release-note and changelog pages use `.entry-list`, `.entry-metadata`, and `.entry-content`.
