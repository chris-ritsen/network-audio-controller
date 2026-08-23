from __future__ import annotations

import hashlib
import sys
import xml.etree.ElementTree as ElementTree
from html.parser import HTMLParser
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit


WEBSITE_DIRECTORY = Path(__file__).resolve().parent
PUBLIC_DIRECTORY = WEBSITE_DIRECTORY / "public"
PAGE_ROUTES = {
    "/": "index.html",
    "/support": "support.html",
    "/privacy": "privacy.html",
}
PAGE_TITLES = {
    "/": "NetAudio | Dante control for iPhone and iPad",
    "/support": "Support | NetAudio",
    "/privacy": "Privacy Policy | NetAudio",
}
BASE_URL = "https://netaudio.app"


class DocumentAnalysis(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.identifiers: list[str] = []
        self.links: list[str] = []
        self.sources: list[str] = []
        self.script_sources: list[str] = []
        self.stylesheet_links: list[str] = []
        self.icon_links: list[str] = []
        self.apple_touch_icon_links: list[str] = []
        self.canonical_url: Optional[str] = None
        self.open_graph_url: Optional[str] = None
        self.meta_description: Optional[str] = None
        self.title_parts: list[str] = []
        self.heading_level_one_count = 0
        self.inline_style_count = 0
        self.inline_script_count = 0
        self.non_deferred_script_count = 0
        self.style_element_count = 0
        self.main_count = 0
        self.header_count = 0
        self.footer_count = 0
        self.primary_navigation_count = 0
        self.current_page_count = 0
        self.skip_link_count = 0
        self.content_page_count = 0
        self.page_contents_count = 0
        self._inside_title = False

    def handle_starttag(self, tag: str, attributes: list[tuple[str, str | None]]) -> None:
        attribute_values = {name: value or "" for name, value in attributes}
        class_names = set(attribute_values.get("class", "").split())
        if "content-page" in class_names:
            self.content_page_count += 1
        if "page-contents" in class_names:
            self.page_contents_count += 1
        identifier = attribute_values.get("id")
        if identifier:
            self.identifiers.append(identifier)
        if "style" in attribute_values:
            self.inline_style_count += 1
        if tag == "script":
            source = attribute_values.get("src")
            if source:
                self.script_sources.append(source)
                if "defer" not in attribute_values:
                    self.non_deferred_script_count += 1
            else:
                self.inline_script_count += 1
        if tag == "style":
            self.style_element_count += 1
        if tag == "title":
            self._inside_title = True
        if tag == "h1":
            self.heading_level_one_count += 1
        if tag == "main":
            self.main_count += 1
        if tag == "header":
            self.header_count += 1
        if tag == "footer":
            self.footer_count += 1
        if tag == "nav" and attribute_values.get("aria-label") == "Primary navigation":
            self.primary_navigation_count += 1
        if attribute_values.get("aria-current") == "page":
            self.current_page_count += 1
        if tag == "a" and attribute_values.get("class") == "skip-link":
            self.skip_link_count += 1
        if tag == "a" and "href" in attribute_values:
            self.links.append(attribute_values["href"])
        if tag in {"img", "source"} and "src" in attribute_values:
            self.sources.append(attribute_values["src"])
        if tag == "link":
            relation_values = set(attribute_values.get("rel", "").split())
            href = attribute_values.get("href", "")
            if "canonical" in relation_values:
                self.canonical_url = href
            if "stylesheet" in relation_values:
                self.stylesheet_links.append(href)
            if "icon" in relation_values:
                self.icon_links.append(href)
            if "apple-touch-icon" in relation_values:
                self.apple_touch_icon_links.append(href)
        if tag == "meta":
            if attribute_values.get("name") == "description":
                self.meta_description = attribute_values.get("content")
            if attribute_values.get("property") == "og:url":
                self.open_graph_url = attribute_values.get("content")

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._inside_title = False

    def handle_data(self, data: str) -> None:
        if self._inside_title:
            self.title_parts.append(data)

    @property
    def title(self) -> str:
        return "".join(self.title_parts).strip()


def analyze_document(file_path: Path) -> DocumentAnalysis:
    analysis = DocumentAnalysis()
    analysis.feed(file_path.read_text(encoding="utf-8"))
    analysis.close()
    return analysis


def canonical_url(route: str) -> str:
    return BASE_URL + route


def versioned_asset_url(asset_name: str) -> str:
    asset_path = PUBLIC_DIRECTORY / "assets" / asset_name
    asset_digest = hashlib.sha256(asset_path.read_bytes()).hexdigest()[:12]
    return f"/assets/{asset_name}?v={asset_digest}"


def local_target(href: str, current_route: str) -> Optional[tuple[str, Optional[str]]]:
    parsed_url = urlsplit(href)
    if parsed_url.scheme or parsed_url.netloc:
        return None
    if href.startswith("mailto:"):
        return None
    target_path = parsed_url.path or current_route
    return target_path, parsed_url.fragment or None


def validate_page(route: str, analyses: dict[str, DocumentAnalysis], failures: list[str]) -> None:
    analysis = analyses[route]
    source_name = PAGE_ROUTES[route]
    if analysis.title != PAGE_TITLES[route]:
        failures.append(f"{source_name}: unexpected title")
    if not analysis.meta_description:
        failures.append(f"{source_name}: missing meta description")
    expected_canonical_url = canonical_url(route)
    if analysis.canonical_url != expected_canonical_url:
        failures.append(f"{source_name}: incorrect canonical URL")
    if analysis.open_graph_url != expected_canonical_url:
        failures.append(f"{source_name}: incorrect Open Graph URL")
    if analysis.heading_level_one_count != 1:
        failures.append(f"{source_name}: expected exactly one h1")
    if analysis.main_count != 1 or analysis.header_count < 1 or analysis.footer_count != 1:
        failures.append(f"{source_name}: incomplete page landmarks")
    if analysis.primary_navigation_count != 1:
        failures.append(f"{source_name}: expected one primary navigation landmark")
    expected_current_page_count = 1 if route == "/" else 2
    if analysis.current_page_count != expected_current_page_count:
        failures.append(f"{source_name}: current page must be identified in header and footer navigation")
    if analysis.skip_link_count != 1 or "main-content" not in analysis.identifiers:
        failures.append(f"{source_name}: missing skip-link target")
    expected_content_page_count = 0 if route == "/" else 1
    expected_page_contents_count = 1 if route == "/privacy" else 0
    if analysis.content_page_count != expected_content_page_count:
        failures.append(f"{source_name}: incorrect shared content-page layout count")
    if analysis.page_contents_count != expected_page_contents_count:
        failures.append(f"{source_name}: incorrect shared contents navigation count")
    if analysis.inline_style_count or analysis.inline_script_count or analysis.style_element_count:
        failures.append(f"{source_name}: inline styling or scripting is not allowed")
    if analysis.non_deferred_script_count:
        failures.append(f"{source_name}: external scripts must be deferred")
    expected_script_sources = [versioned_asset_url("contents-navigation.js")] if route == "/privacy" else []
    if analysis.script_sources != expected_script_sources:
        failures.append(f"{source_name}: unexpected external scripts")
    if analysis.stylesheet_links != [versioned_asset_url("site.css")]:
        failures.append(f"{source_name}: expected the shared stylesheet only")
    if analysis.icon_links != [versioned_asset_url("favicon.svg"), versioned_asset_url("favicon.ico")]:
        failures.append(f"{source_name}: favicon versions do not match their contents")
    if analysis.apple_touch_icon_links != [versioned_asset_url("apple-touch-icon.png")]:
        failures.append(f"{source_name}: Apple touch icon version does not match its contents")
    if len(analysis.identifiers) != len(set(analysis.identifiers)):
        failures.append(f"{source_name}: duplicate identifiers")

    for href in analysis.links:
        if not href:
            failures.append(f"{source_name}: empty link")
            continue
        target = local_target(href, route)
        if target is None:
            continue
        target_path, target_identifier = target
        if target_path in PAGE_ROUTES:
            target_analysis = analyses[target_path]
        elif target_path in {"/robots.txt", "/sitemap.xml"}:
            target_analysis = None
            if not (PUBLIC_DIRECTORY / target_path.removeprefix("/")).is_file():
                failures.append(f"{source_name}: missing target {target_path}")
        elif target_path.startswith("/assets/"):
            target_analysis = None
            if not (PUBLIC_DIRECTORY / target_path.removeprefix("/")).is_file():
                failures.append(f"{source_name}: missing asset {target_path}")
        else:
            failures.append(f"{source_name}: unknown internal target {target_path}")
            continue
        if target_identifier and target_analysis is not None and target_identifier not in target_analysis.identifiers:
            failures.append(f"{source_name}: missing fragment target {href}")

    for source in analysis.sources:
        target = local_target(source, route)
        if target is None:
            continue
        source_path, _ = target
        if not source_path.startswith("/assets/") or not (PUBLIC_DIRECTORY / source_path.removeprefix("/")).is_file():
            failures.append(f"{source_name}: missing source asset {source}")

    for source in analysis.script_sources:
        target = local_target(source, route)
        if target is None:
            failures.append(f"{source_name}: script must be a local asset")
            continue
        source_path, _ = target
        if not source_path.startswith("/assets/") or not (PUBLIC_DIRECTORY / source_path.removeprefix("/")).is_file():
            failures.append(f"{source_name}: missing script asset {source}")


def validate_content(failures: list[str]) -> None:
    home_markup = (PUBLIC_DIRECTORY / "index.html").read_text(encoding="utf-8")
    support_markup = (PUBLIC_DIRECTORY / "support.html").read_text(encoding="utf-8")
    privacy_markup = (PUBLIC_DIRECTORY / "privacy.html").read_text(encoding="utf-8")
    required_home_text = [
        "Control Dante networks from iPhone and iPad.",
        "NetAudio is currently under development.",
    ]
    forbidden_home_text = [
        "Available now",
        "Download on the App Store",
        "Join TestFlight",
        "Pricing available",
        "Not yet generally available",
        "What NetAudio does",
        "Local by design",
        "Support and privacy",
    ]
    required_support_text = [
        "Get help with NetAudio, report an app or device compatibility issue, or suggest an improvement.",
        "Settings → Support → Feedback &amp; Feature Requests",
        "Diagnostics are optional.",
        "support@netaudio.app",
    ]
    required_privacy_text = [
        "Information discovered from your audio devices stays on your device and local network unless you deliberately submit feedback that includes it.",
        "Current relay connections use unencrypted HTTP and server-sent events on the local network",
        "The diagnostic privacy level does not make the entire submission anonymous.",
        "There is currently no fixed automatic deletion schedule for submitted feedback or App Attest registration records",
        "Cloudflare enables Network Error Logging",
    ]
    forbidden_privacy_text = [
        "Cloudflare Web Analytics",
        "NetAudio is developed and operated by Christopher Michael Ritsen",
        "Activity entries and presets have in-app deletion controls",
        "This policy explains how the NetAudio iOS app",
        '<h2 id="summary-heading">Summary</h2>',
        'href="#summary"',
    ]
    for required_text in required_home_text:
        if required_text not in home_markup:
            failures.append(f"index.html: missing required text: {required_text}")
    for forbidden_text in forbidden_home_text:
        if forbidden_text in home_markup:
            failures.append(f"index.html: forbidden availability claim: {forbidden_text}")
    for required_text in required_support_text:
        if required_text not in support_markup:
            failures.append(f"support.html: missing preserved support text: {required_text}")
    for required_text in required_privacy_text:
        if required_text not in privacy_markup:
            failures.append(f"privacy.html: missing preserved policy text: {required_text}")
    for forbidden_text in forbidden_privacy_text:
        if forbidden_text in privacy_markup:
            failures.append(f"privacy.html: stale or unwanted policy text remains: {forbidden_text}")
    if 'class="documentation-heading"' in support_markup:
        failures.append("support.html: obsolete two-column heading wrapper remains")


def validate_assets(failures: list[str]) -> None:
    stylesheet = (PUBLIC_DIRECTORY / "assets" / "site.css").read_text(encoding="utf-8").lower()
    for forbidden_rule in ["opacity:", "text-overflow", "line-clamp", "overflow: hidden"]:
        if forbidden_rule in stylesheet:
            failures.append(f"site.css: forbidden truncation or dimming rule: {forbidden_rule}")
    for obsolete_selector in [".document-layout", ".document-contents", ".document-summary", ".section-navigation"]:
        if obsolete_selector in stylesheet:
            failures.append(f"site.css: obsolete page-specific layout remains: {obsolete_selector}")
    favicon_path = PUBLIC_DIRECTORY / "assets" / "favicon.svg"
    try:
        favicon_root = ElementTree.parse(favicon_path).getroot()
    except ElementTree.ParseError:
        failures.append("favicon.svg: invalid XML")
    else:
        favicon_elements = list(favicon_root)
        if (
            len(favicon_elements) != 1
            or not favicon_elements[0].tag.endswith("rect")
            or favicon_elements[0].get("fill") != "#000"
        ):
            failures.append("favicon.svg: favicon must be a solid black tile")
    for required_asset in ["favicon.ico", "apple-touch-icon.png", "contents-navigation.js"]:
        asset_path = PUBLIC_DIRECTORY / "assets" / required_asset
        if not asset_path.is_file() or asset_path.stat().st_size == 0:
            failures.append(f"assets: missing {required_asset}")


def validate_sitemap_and_robots(failures: list[str]) -> None:
    sitemap_path = PUBLIC_DIRECTORY / "sitemap.xml"
    try:
        sitemap_root = ElementTree.parse(sitemap_path).getroot()
    except ElementTree.ParseError:
        failures.append("sitemap.xml: invalid XML")
    else:
        namespace = {"sitemap": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        locations = [element.text for element in sitemap_root.findall("sitemap:url/sitemap:loc", namespace)]
        expected_locations = [canonical_url(route) for route in PAGE_ROUTES]
        if locations != expected_locations:
            failures.append("sitemap.xml: routes do not match the public pages")
    expected_robots = "User-agent: *\nAllow: /\nSitemap: https://netaudio.app/sitemap.xml\n"
    if (PUBLIC_DIRECTORY / "robots.txt").read_text(encoding="utf-8") != expected_robots:
        failures.append("robots.txt: unexpected contents")


def validate_website() -> list[str]:
    failures: list[str] = []
    analyses = {route: analyze_document(PUBLIC_DIRECTORY / source_name) for route, source_name in PAGE_ROUTES.items()}
    for route in PAGE_ROUTES:
        validate_page(route, analyses, failures)
    validate_content(failures)
    validate_assets(failures)
    validate_sitemap_and_robots(failures)
    return failures


def main() -> None:
    failures = validate_website()
    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        raise SystemExit(1)
    print("Validated 3 pages and shared website assets")


if __name__ == "__main__":
    main()
