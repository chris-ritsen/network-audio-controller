import { Fields, OnlineState, Panel, Value } from "../components.js";
import { aes67Status } from "../aes67.js";
import * as format from "../format.js";
import { html } from "../lib/preact.js";

export function StatusSection({ device }) {
  const subscriptions = (device.subscriptions || []).filter(
    (entry) => entry.tx_device,
  );
  const problems = subscriptions.filter(
    (entry) => entry.status && entry.status.severity !== "ok",
  );
  const clock = device.clock_status || {};
  const fresh = format.clockStatusFresh(device);
  return html`
    <div class="flex flex-col gap-4">
      <div class="split">
        <${Panel} title="Device">
          <${Fields}
            entries=${[
              ["State", html`<${OnlineState} online=${device.online} />`],
              ["Manufacturer", html`<${Value} value=${device.manufacturer} />`],
              [
                "Model",
                html`<${Value} value=${format.deviceModelName(device)} />`,
              ],
              [
                "Dante platform model",
                html`<${Value} value=${device.platform_model_name} />`,
              ],
              [
                "Product version",
                html`<${Value}
                  value=${device.friendly_product_version || device.product_version}
                />`,
              ],
              [
                "Numeric product version",
                html`<${Value}
                  value=${device.friendly_product_version ? device.product_version : null}
                />`,
              ],
              [
                "Manufacturer software version",
                html`<${Value}
                  value=${device.manufacturer_software_version}
                />`,
              ],
              [
                "Manufacturer firmware version",
                html`<${Value}
                  value=${device.manufacturer_firmware_version}
                />`,
              ],
              [
                device.platform_hardware_version
                  ? "Dante firmware version"
                  : "Dante software version",
                html`<${Value} value=${device.platform_software_version} />`,
              ],
              [
                "Hardware version",
                html`<${Value} value=${device.platform_hardware_version} />`,
              ],
              [
                "ROM/Boot version",
                html`<${Value} value=${device.rom_boot_version} />`,
              ],
              [
                "Dante API version",
                html`<${Value} value=${device.platform_api_version} />`,
              ],
              [
                "CMC server version (DNS-SD)",
                html`<${Value} value=${device.cmc_server_version} />`,
              ],
              [
                "Router protocol version (DNS-SD)",
                html`<${Value} value=${device.router_protocol_version} />`,
              ],
              [
                "DDM product version",
                html`<${Value} value=${device.ddm_product_version} />`,
              ],
              [
                "DDM product software version",
                html`<${Value} value=${device.ddm_product_software_version} />`,
              ],
              [
                "DDM Dante version",
                html`<${Value} value=${device.ddm_dante_version} />`,
              ],
              [
                "DDM Dante hardware version",
                html`<${Value} value=${device.ddm_dante_hardware_version} />`,
              ],
              ["Primary address", html`<${Value} value=${device.ipv4} />`],
              ["MAC address", format.macAddress(device)],
              [
                "Device lock",
                device.is_locked == null
                  ? "Unknown"
                  : device.is_locked
                    ? "Locked"
                    : "Unlocked",
              ],
              ["Last seen", format.timestamp(device.last_seen)],
            ]}
          />
        <//>
        <${Panel} title="Audio and clock">
          <${Fields}
            entries=${[
              ["Sample rate", format.sampleRate(device.sample_rate_hz)],
              [
                "Encoding",
                device.encoding ? `PCM ${device.encoding}` : format.ABSENT,
              ],
              ["Latency", format.latency(device.latency_ms)],
              [
                "Synchronization",
                fresh ? clock.synchronization : "unavailable",
              ],
              [
                "Clock mute",
                fresh && clock.mute_flags != null
                  ? clock.mute_flags === 0
                    ? "none"
                    : `${(clock.mute_reasons || []).join(", ")} (0x${clock.mute_flags.toString(16)})`
                  : "unavailable",
              ],
              [
                "Servo",
                clock.servo_state ?? clock.servo_state_code ?? "unavailable",
              ],
              ["PTPv1 device UUID", device.ptpv1_device_uuid || "unavailable"],
              [
                "PTPv1 current master UUID",
                device.ptpv1_master_uuid || "unavailable",
              ],
              [
                "PTPv1 grandmaster UUID",
                device.ptpv1_grandmaster_uuid || "unavailable",
              ],
              ["PTPv2 domain", String(clock.ptpv2_domain ?? "unavailable")],
              [
                "Word clock",
                clock.word_clock_state ??
                  clock.word_clock_state_code ??
                  "unavailable",
              ],
              [
                "Frequency offset",
                clock.clock_frequency_offset_parts_per_billion == null
                  ? "unavailable"
                  : `${clock.clock_frequency_offset_parts_per_billion} ppb`,
              ],
              ["Clock role", html`<${Value} value=${device.clock_role} />`],
              [
                "Preferred leader",
                format.preferredLeader(device.preferred_leader),
              ],
              [
                "Clock source",
                format.clockSourceCode(device.clock_source_code),
              ],
              [
                "Clock subdomain",
                format.clockSubdomain(device.clock_subdomain),
              ],
              ["AES67", aes67Status(device).label],
            ]}
          />
        <//>
      </div>
      <${Panel} title="Clock ports">
        <${Fields}
          entries=${(
            clock.clock_port_records ||
            (clock.base_ports || []).map((port, index) => ({
              ...port,
              record_number: index + 1,
            }))
          ).map((port) => [
            `Port ${port.record_number}${port.ptp_version == null ? "" : ` · PTPv${port.ptp_version}`}`,
            `${port.state || port.state_code} · ${port.transport_path ?? port.transport_path_code ?? "path unavailable"} · interface ${port.network_interface_index == null ? "unavailable" : port.network_interface_index === 0 ? "primary" : port.network_interface_index === 1 ? "secondary" : port.network_interface_index} · link ${port.link_down == null ? "unavailable" : port.link_down ? "down" : "up"} · disabled ${port.user_disabled ?? "unavailable"} · unicast delay ${port.unicast_delay_requests ?? "unavailable"}`,
          ])}
        />
      <//>
      <${Panel} title="Channels and subscriptions">
        <${Fields}
          entries=${[
            ["Transmitters", html`<${Value} value=${device.tx_count} />`],
            ["Receivers", html`<${Value} value=${device.rx_count} />`],
            ["Subscriptions", String(subscriptions.length)],
            [
              "Subscription problems",
              problems.length
                ? html`<span class="state-warn"
                    >${problems.length} —
                    ${problems.map((entry) => `${entry.rx_channel}: ${format.subscriptionStatusText(entry)}`).join("; ")}</span
                  >`
                : "none",
            ],
          ]}
        />
      <//>
    </div>
  `;
}
