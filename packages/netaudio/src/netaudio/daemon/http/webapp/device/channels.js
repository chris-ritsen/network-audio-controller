import { api } from "../api.js";
import {AsyncButton, Button, Notice, Panel, Value} from "../components.js";
import * as format from "../format.js";
import { html, useEffect, useLayoutEffect, useRef, useState } from "../lib/preact.js";
import { RoutePicker } from "../route-picker.js";
import { deviceRequestName } from "../store.js";
import { ConfigurableTable } from "../table.js";
import { SubscriptionStatus } from "./receiver-status.js";

function gainChannelType(device) {
  if (device.gain_device_type === "input") {
    return "tx";
  }
  if (device.gain_device_type === "output") {
    return "rx";
  }
  return null;
}

function NameCell({ channel, channelNumber, channelType, requestName }) {
  const input = useRef(null);
  const editor = useRef(null);
  const [editing, setEditing] = useState(false);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState("");
  const [name, setName] = useState(channel.name || "");
  // Leave the editable text node under browser control so rerenders do not move the caret.
  const draft = useRef("");
  const label = `${channelType === "rx" ? "Receive" : "Transmit"} channel ${channelNumber} name`;
  useEffect(() => setName(channel.name || ""), [channel.name]);
  useLayoutEffect(() => {
    if (editing) {
      input.current?.focus();
      const selection = window.getSelection();
      const range = document.createRange();
      range.selectNodeContents(input.current);
      selection.removeAllRanges();
      selection.addRange(range);
      const reposition = () => {
        const box = input.current.getBoundingClientRect();
        const above = window.innerHeight - box.bottom < 80;
        editor.current.style.left = `${box.left}px`;
        editor.current.style.top = `${above ? box.top : box.bottom}px`;
        editor.current.style.width = `${box.width}px`;
        editor.current.classList.toggle("actions-above", above);
        editor.current.classList.toggle("actions-right", window.innerWidth - box.left < 248);
      };
      reposition();
      window.addEventListener("resize", reposition);
      window.addEventListener("scroll", reposition, true);
      const outside = (event) => {
        if (!input.current.contains(event.target) && !editor.current.contains(event.target)) close();
      };
      document.addEventListener("pointerdown", outside);
      return () => {
        window.removeEventListener("resize", reposition);
        window.removeEventListener("scroll", reposition, true);
        document.removeEventListener("pointerdown", outside);
      };
    }
  }, [editing]);
  const close = (value = name) => {
    editor.current.hidePopover();
    input.current.textContent = value || "Unnamed channel";
    input.current.style.width = "";
    setEditing(false);
    setError("");
    requestAnimationFrame(() => input.current?.focus());
  };
  const save = async (value) => {
    if (pending) return;
    setPending(true);
    setError("");
    try {
      await api.renameChannel(requestName, channelType, channelNumber, value);
      if (value) setName(value);
      close(value || name);
    } catch (failure) {
      setError(failure.message);
    } finally {
      setPending(false);
    }
  };
  const open = () => {
    if (editing) return;
    const box = input.current.getBoundingClientRect();
    input.current.style.width = `${box.width}px`;
    input.current.textContent = name;
    draft.current = name;
    setError("");
    setEditing(true);
    editor.current.showPopover();
  };
  return html`
    <span class="channel-name-display">
      <span
        ref=${input}
        class=${`channel-name-value${editing ? " channel-name-input" : ""}`}
        role=${editing ? "textbox" : "button"}
        tabIndex="0"
        contentEditable=${editing && !pending ? "plaintext-only" : "false"}
        spellCheck="false"
        aria-multiline=${editing ? "false" : null}
        aria-label=${editing ? label : `Edit ${label.toLowerCase()}`}
        aria-placeholder="Default channel name"
        aria-disabled=${pending}
        title=${editing ? null : "Click to edit"}
        onClick=${open}
        onKeyDown=${(event) => {
          if (event.isComposing || pending) return;
          if (!editing && (event.key === "Enter" || event.key === " ")) { event.preventDefault(); open(); }
          else if (editing && event.key === "Enter") { event.preventDefault(); void save(draft.current); }
          else if (editing && event.key === "Escape") { event.preventDefault(); close(); }
        }}
        onInput=${(event) => {
          const value = event.currentTarget.innerText.replace(/[\r\n]+$/g, "").replace(/[\r\n]+/g, " ");
          if (event.currentTarget.textContent !== value) event.currentTarget.textContent = value;
          draft.current = value;
        }}
      >${name || "Unnamed channel"}</span>
    <form ref=${editor} popover="manual" class="channel-name-editor"
      onSubmit=${(event) => { event.preventDefault(); void save(draft.current); }} onKeyDown=${(event) => {
        if (event.key === "Escape" && !pending) { event.preventDefault(); close(); }
      }}>
      <div class="channel-name-actions">
      <button type="submit" class="btn btn-xs" disabled=${pending} aria-busy=${pending}>Save</button>
      <${Button} small disabled=${pending} onClick=${() => close()}>Cancel<//>
      ${error ? html`<span role="alert" class="text-error text-sm">${error}</span>` : null}
      </div>
    </form>
    </span>
  `;
}

function GainCell({ channel, channelNumber, channelType, device, requestName }) {
  const select = useRef(null);
  const choices = device.gain_level_choices;
  if (!choices || gainChannelType(device) !== channelType) {
    return html`<span>${html`<${Value} value=${channel.gain_level_label} />`}</span>`;
  }
  return html`
    <span class="cell-actions">
      <select key=${`gain-${requestName}-${channelType}-${channelNumber}`} ref=${select}>
        ${choices.map((choice) => {
          const value = choice !== null && typeof choice === "object" ? choice.value : choice;
          const label = choice !== null && typeof choice === "object" ? choice.label : String(choice);
          return html`<option key=${value} value=${value} selected=${Number(value) === Number(channel.gain_level)}>
            ${label}
          </option>`;
        })}
      </select>
      <${AsyncButton}
        small
        description=${`set gain on ${requestName} channel ${channelNumber}`}
        onRun=${() => api.setGain(requestName, channelNumber, Number(select.current.value), device.gain_device_type)}
      >
        Set
      <//>
    </span>
  `;
}

function subscriptionForChannel(device, channel) {
  return (device.subscriptions || []).find((entry) => entry.rx_channel === channel.name) || null;
}

function receiveColumns(device, requestName, onRoute) {
  return [
    { align: "right", cell: (row) => row.number, id: "number", label: "#" },
    {
      cell: (row) =>
        html`<${NameCell} key=${`${requestName}:rx:${row.number}`} channel=${row.channel} channelNumber=${row.number} channelType="rx" requestName=${requestName} />`,
      id: "name",
      label: "Name",
    },
    { cell: (row) => format.subscriptionSource(row.subscription), id: "subscription", label: "Subscription" },
    {
      cell: (row) => html`<${SubscriptionStatus} subscription=${row.subscription} />`,
      id: "status",
      label: "Status",
    },
    {
      cell: (row) => html`
        <span class="cell-actions">
          <${Button} small onClick=${() => onRoute(row)}>Subscribe<//>
          ${row.subscription && row.subscription.tx_device
            ? html`<${AsyncButton}
                small
                description=${`unsubscribe ${requestName} channel ${row.number}`}
                onRun=${() => api.unsubscribe({ rx_channel: row.number, rx_device: requestName })}
              >
                Unsubscribe
              <//>`
            : null}
        </span>
      `,
      id: "subscribe",
      label: "Subscribe",
    },
    {
      cell: (row) =>
        html`<${GainCell}
          channel=${row.channel}
          channelNumber=${row.number}
          channelType="rx"
          device=${device}
          requestName=${requestName}
        />`,
      id: "gain",
      label: "Gain",
    },
    { cell: (row) => html`<${Value} value=${row.channel.volume} />`, id: "volume", label: "Volume", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.muted} />`, id: "muted", label: "Muted", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.media_type} />`, id: "media-type", label: "Media type", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.status_text} />`, id: "channel-status", label: "Channel status", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.ddm_summary} />`, id: "managed", label: "Managed", defaultHidden: true },
  ];
}

function transmitColumns(device, requestName) {
  return [
    { align: "right", cell: (row) => row.number, id: "number", label: "#" },
    {
      cell: (row) =>
        html`<${NameCell} key=${`${requestName}:tx:${row.number}`} channel=${row.channel} channelNumber=${row.number} channelType="tx" requestName=${requestName} />`,
      id: "name",
      label: "Name",
    },
    { cell: (row) => html`<${Value} value=${row.channel.friendly_name} />`, id: "friendly-name", label: "Friendly name", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.factory_name} />`, id: "factory-name", label: "Factory name", defaultHidden: true },
    {
      cell: (row) =>
        html`<${GainCell}
          channel=${row.channel}
          channelNumber=${row.number}
          channelType="tx"
          device=${device}
          requestName=${requestName}
        />`,
      id: "gain",
      label: "Gain",
    },
    { cell: (row) => format.sampleRate(row.channel.sample_rate), id: "sample-rate", label: "Sample rate", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.encoding} />`, id: "encoding", label: "Encoding", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.bit_depth} />`, id: "bit-depth", label: "Bit depth", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.media_type} />`, id: "media-type", label: "Media type", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.status_text} />`, id: "channel-status", label: "Channel status", defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.ddm_summary} />`, id: "managed", label: "Managed", defaultHidden: true },
  ];
}

function receiveRows(device) {
  const receiveChannels = device.channels ? device.channels.receivers || {} : {};
  return format.sortedChannelNumbers(receiveChannels).map((number) => ({
    channel: receiveChannels[number],
    number,
    subscription: subscriptionForChannel(device, receiveChannels[number]),
  }));
}

function transmitRows(device) {
  const transmitChannels = device.channels ? device.channels.transmitters || {} : {};
  return format.sortedChannelNumbers(transmitChannels).map((number) => ({ channel: transmitChannels[number], number }));
}

export function ReceiveSection({ device }) {
  const requestName = deviceRequestName(device);
  const [routing, setRouting] = useState(null);
  const rows = receiveRows(device);
  return html`
    <div class="flex flex-col gap-4">
      ${routing
        ? html`<${RoutePicker}
            receiver=${device}
            receiveChannelNumber=${routing.number}
            receiveChannelName=${routing.channel.name}
            subscription=${routing.subscription}
            onClose=${() => setRouting(null)}
          />`
        : null}
      <${Panel}
        title=${`Receivers (${rows.length})`}
        headerActions=${rows.length
          ? html`<${AsyncButton}
              small
              variant="danger"
              description=${`unsubscribe all receivers on ${requestName}`}
              onRun=${() => api.unsubscribe({ rx_channels: rows.map((row) => row.number), rx_device: requestName })}
            >
              Unsubscribe all
            <//>`
          : null}
      >
        ${rows.length === 0
          ? html`<${Notice}>This device reports no Dante receivers.<//>`
          : html`<${ConfigurableTable}
              tableId="device-receive-channels"
              mobileSummary=${(row) => ({
                title: html`<span class="receiver-summary-line">
                  <${SubscriptionStatus} subscription=${row.subscription} />
                  <span>${row.number}. ${row.channel.name || "Unnamed channel"}</span>
                </span>`,
                detail: row.subscription?.tx_device ? format.subscriptionSource(row.subscription) : "Not subscribed",
              })}
              columns=${receiveColumns(device, requestName, setRouting).filter((column) => column.id !== "gain" || (gainChannelType(device) === "rx" && device.gain_level_choices?.length))}
              rows=${rows}
              rowKey=${(row) => row.number}
            />`}
      <//>
    </div>
  `;
}

export function TransmitSection({ device }) {
  const requestName = deviceRequestName(device);
  const rows = transmitRows(device);
  return html`
    <div class="flex flex-col gap-4">
    <${Panel} title=${`Transmitters (${rows.length})`}>
      ${rows.length === 0
        ? html`<${Notice}>This device reports no Dante transmitters.<//>`
        : html`<${ConfigurableTable}
            tableId="device-transmit-channels"
            mobileSummary=${(row) => ({ title: html`<span class="receiver-summary-line"><span>${row.number}. ${row.channel.name || "Unnamed channel"}</span></span>` })}
            columns=${transmitColumns(device, requestName).filter((column) => column.id !== "gain" || (gainChannelType(device) === "tx" && device.gain_level_choices?.length))}
            rows=${rows}
            rowKey=${(row) => row.number}
          />`}
    <//>
    </div>
  `;
}
