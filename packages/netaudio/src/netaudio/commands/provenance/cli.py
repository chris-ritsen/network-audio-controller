import typer

from netaudio.cli_support.context import HELP_CONTEXT_SETTINGS
from netaudio.commands.provenance import analysis, catalog, evidence, network

app = typer.Typer(
    help="Wire-observation provenance workflows.", no_args_is_help=True, context_settings=HELP_CONTEXT_SETTINGS
)

app.command("analysis", help="Record an analysis marker with extracted packet fields.")(analysis.provenance_analysis)
app.command("analyze", help="Dissect every packet in a provenance bundle.")(analysis.provenance_analyze)
app.command("artifact", help="Attach a curated file as an evidence artifact.")(evidence.provenance_artifact)
app.command("audit", help="Audit bundles for missing evidence or inconsistent metadata.")(catalog.provenance_audit)
app.command("check", help="Check fixture payload samples against the label catalog.")(catalog.provenance_check)
app.command("evidence", help="Tag captured packets as evidence with a label and note.")(evidence.provenance_evidence)
app.command("export", help="Export a capture session as a provenance bundle.")(catalog.provenance_export)
app.command("hypothesis", help="Record a falsifiable hypothesis marker in a session.")(analysis.provenance_hypothesis)
app.command("ingest-packet", help="Copy packets from another capture database as evidence.")(
    evidence.provenance_ingest_packet
)
app.command("ingest-payload", help="Ingest one raw UDP payload file as evidence.")(evidence.provenance_ingest_payload)
app.command("ingest-pcap", help="Ingest selected frames from a pcap file as evidence.")(evidence.provenance_ingest_pcap)
app.command("label", help="Maintain opcode, message, and status labels for captured packets.")(catalog.provenance_label)
app.command("replay", help="Replay a provenance bundle's requests against a device.")(network.provenance_replay)
app.command("seed", help="Write payload fixtures from captured packets into a fixture directory.")(
    catalog.provenance_seed
)
app.command("send", help="Send a payload to a device and record the exchange as evidence.")(network.provenance_send)
app.command("show", help="Show the contents of a provenance bundle.")(catalog.provenance_show)
app.command("verify", help="Verify bundle checksums and manifests.")(catalog.provenance_verify)
