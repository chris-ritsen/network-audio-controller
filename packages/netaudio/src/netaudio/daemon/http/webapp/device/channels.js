import { api } from "../api.js";
import { AsyncButton, Button, Panel, Value } from "../components.js";
import * as format from "../format.js";
import { t } from "../i18n.js";
import { html, useEffect, useLayoutEffect, useRef, useState } from "../lib/preact.js";
import { RoutePicker } from "../route-picker.js";
import { deviceRequestName } from "../store.js";
import { ConfigurableTable } from "../table.js";
import { operationWritable } from "./availability.js";
import { SubscriptionStatus } from "./receiver-status.js";
import { ReceiverFlows, TransmitFlows } from "./flows.js";

function gainChannelType(device) {
  if (device.gain_device_type === "input") {
    return "tx";
  }
  if (device.gain_device_type === "output") {
    return "rx";
  }
  return null;
}

function NameCell({ channel, channelNumber, channelType, requestName, managed }) {
  const input = useRef(null);
  const editor = useRef(null);
  const [editing, setEditing] = useState(false);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState("");
  const [name, setName] = useState(channel.name || "");
  // Leave the editable text node under browser control so rerenders do not move the caret.
  const draft = useRef("");
  const label = channelType === "rx"
    ? t("Receive channel {number} name", { number: channelNumber })
    : t("Transmit channel {number} name", { number: channelNumber });
  const editLabel = channelType === "rx"
    ? t("Edit receive channel {number} name", { number: channelNumber })
    : t("Edit transmit channel {number} name", { number: channelNumber });
  const renameAllowed = channelType !== "rx" || managed || channel.can_rename === true;
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
    input.current.textContent = value || t("Unnamed channel");
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
    if (editing || !renameAllowed) return;
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
        role=${editing ? "textbox" : renameAllowed ? "button" : null}
        tabIndex=${renameAllowed ? "0" : null}
        contentEditable=${editing && !pending ? "plaintext-only" : "false"}
        spellCheck="false"
        aria-multiline=${editing ? "false" : null}
        aria-label=${editing ? label : editLabel}
        aria-placeholder=${t("Default channel name")}
        aria-disabled=${pending || !renameAllowed}
        title=${editing || !renameAllowed ? null : t("Click to edit")}
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
      >${name || t("Unnamed channel")}</span>
    <form ref=${editor} popover="manual" class="channel-name-editor"
      onSubmit=${(event) => { event.preventDefault(); void save(draft.current); }} onKeyDown=${(event) => {
        if (event.key === "Escape" && !pending) { event.preventDefault(); close(); }
      }}>
      <div class="channel-name-actions">
      <button type="submit" class="btn btn-xs" disabled=${pending} aria-busy=${pending}>${t("Save")}</button>
      <${Button} small disabled=${pending} onClick=${() => close()}>${t("Cancel")}<//>
      ${error ? html`<span role="alert" class="text-error text-sm">${t(error)}</span>` : null}
      </div>
    </form>
    </span>
  `;
}

function GainCell({ channel, channelNumber, channelType, device, requestName }) {
  const select = useRef(null);
  const choices = device.gain_level_choices;
  if (!choices || gainChannelType(device) !== channelType) {
    return html`<span>${html`<${Value} value=${t(channel.gain_level_label)} />`}</span>`;
  }
  if (!operationWritable(device, "codec_control")) {
    return html`<${Value} value=${t(channel.gain_level_label)} />`;
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
        description=${t("set gain on {device} channel {number}", { device: requestName, number: channelNumber })}
        onRun=${() => api.setGain(requestName, channelNumber, Number(select.current.value), device.gain_device_type)}
      >
        ${t("Set")}
      <//>
    </span>
  `;
}

function subscriptionForChannel(device, number) {
  return (device.subscriptions || []).find((entry) => entry.rx_channel_number === number) || null;
}

function receiveColumns(device, requestName, onRoute) {
  return [
    { align: "right", cell: (row) => row.number, id: "number", label: "#" },
    {
      cell: (row) =>
        html`<${NameCell} key=${`${requestName}:rx:${row.number}`} channel=${row.channel} channelNumber=${row.number} channelType="rx" requestName=${requestName} managed=${device.requires_managed_control === true} />`,
      id: "name",
      label: t("Name"),
    },
    { cell: (row) => format.subscriptionSource(row.subscription), id: "subscription", label: t("Subscription") },
    {
      cell: (row) => html`<${SubscriptionStatus} subscription=${row.subscription} />`,
      id: "status",
      label: t("Status"),
    },
    {
      cell: (row) => html`
        <span class="cell-actions">
          <${Button} small onClick=${() => onRoute(row)}>${t("Subscribe")}<//>
          ${row.subscription && row.subscription.tx_device
            ? html`<${AsyncButton}
                small
                description=${t("unsubscribe {device} channel {number}", { device: requestName, number: row.number })}
                onRun=${() => api.unsubscribe({ rx_channel: row.number, rx_device: requestName })}
              >
                ${t("Unsubscribe")}
              <//>`
            : null}
        </span>
      `,
      id: "subscribe",
      label: t("Subscribe"),
      sortable: false,
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
      label: t("Gain"),
    },
    { cell: (row) => html`<${Value} value=${row.channel.volume} />`, id: "volume", label: t("Volume"), defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.muted} />`, id: "muted", label: t("Muted"), defaultHidden: true },
    { cell: (row) => html`<${Value} value=${t(row.channel.media_type)} />`, id: "media-type", label: t("Media type"), defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.status_text} />`, id: "channel-status", label: t("Channel status"), defaultHidden: true },
    { cell: (row) => html`<${Value} value=${t(row.channel.ddm_summary)} />`, id: "managed", label: t("Managed"), defaultHidden: true },
  ];
}

function transmitColumns(device, requestName) {
  return [
    { align: "right", cell: (row) => row.number, id: "number", label: "#" },
    {
      cell: (row) =>
        html`<${NameCell} key=${`${requestName}:tx:${row.number}`} channel=${row.channel} channelNumber=${row.number} channelType="tx" requestName=${requestName} />`,
      id: "name",
      label: t("Name"),
    },
    { cell: (row) => html`<${Value} value=${row.channel.friendly_name} />`, id: "friendly-name", label: t("Friendly name"), defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.factory_name} />`, id: "factory-name", label: t("Factory name"), defaultHidden: true },
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
      label: t("Gain"),
    },
    { cell: (row) => format.sampleRate(row.channel.sample_rate), id: "sample-rate", label: t("Sample rate"), defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.encoding} />`, id: "encoding", label: t("Encoding"), defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.bit_depth} />`, id: "bit-depth", label: t("Bit depth"), defaultHidden: true },
    { cell: (row) => html`<${Value} value=${t(row.channel.media_type)} />`, id: "media-type", label: t("Media type"), defaultHidden: true },
    { cell: (row) => html`<${Value} value=${row.channel.status_text} />`, id: "channel-status", label: t("Channel status"), defaultHidden: true },
    { cell: (row) => html`<${Value} value=${t(row.channel.ddm_summary)} />`, id: "managed", label: t("Managed"), defaultHidden: true },
  ];
}

function receiveRows(device) {
  const receiveChannels = device.channels ? device.channels.receivers || {} : {};
  return format.sortedChannelNumbers(receiveChannels).map((number) => ({
    channel: receiveChannels[number],
    number,
    subscription: subscriptionForChannel(device, number),
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
      ${rows.length ? html`<${Panel}
        title=${t("Receivers ({count})", { count: rows.length })}
        headerActions=${rows.length
          ? html`<${AsyncButton}
              small
              variant="danger"
              description=${t("unsubscribe all receivers on {device}", { device: requestName })}
              onRun=${() => api.unsubscribe({ rx_channels: rows.map((row) => row.number), rx_device: requestName })}
            >
              ${t("Unsubscribe all")}
            <//>`
          : null}
      >
        <${ConfigurableTable}
              tableId="device-receive-channels"
              mobileSummary=${(row) => ({
                title: html`<span class="receiver-summary-line">
                  <${SubscriptionStatus} subscription=${row.subscription} />
                  <span>${row.number}. ${row.channel.name || t("Unnamed channel")}</span>
                </span>`,
                detail: row.subscription?.tx_device ? format.subscriptionSource(row.subscription) : t("Not subscribed"),
              })}
              columns=${receiveColumns(device, requestName, setRouting).filter((column) => column.id !== "gain" || (gainChannelType(device) === "rx" && device.gain_level_choices?.length))}
              rows=${rows}
              rowKey=${(row) => row.number}
            />
      <//>` : null}
      <${ReceiverFlows} device=${device} />
    </div>
  `;
}

export function TransmitSection({ device }) {
  const requestName = deviceRequestName(device);
  const rows = transmitRows(device);
  return html`
    <div class="flex flex-col gap-4">
    ${rows.length ? html`<${Panel} title=${t("Transmitters ({count})", { count: rows.length })}>
      <${ConfigurableTable}
            tableId="device-transmit-channels"
            mobileSummary=${(row) => ({ title: html`<span class="receiver-summary-line"><span>${row.number}. ${row.channel.name || t("Unnamed channel")}</span></span>` })}
            columns=${transmitColumns(device, requestName).filter((column) => column.id !== "gain" || (gainChannelType(device) === "tx" && device.gain_level_choices?.length))}
            rows=${rows}
            rowKey=${(row) => row.number}
          />
    <//>` : null}
    <${TransmitFlows} device=${device} />
    </div>
  `;
}
