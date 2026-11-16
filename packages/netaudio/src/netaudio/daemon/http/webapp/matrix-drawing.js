import * as format from "./format.js";
import {
  CELL,
  GUTTER_PADDING,
  HEADER_PADDING,
  INDENT,
  MINIMUM_GUTTER,
  MINIMUM_HEADER,
  cellState,
  expanded,
  severityName,
  severityRank,
} from "./matrix-model.js";
import { stickyDevice } from "./matrix.js";

export function readTheme() {
  const style =
    typeof getComputedStyle === "function" && document.documentElement
      ? getComputedStyle(document.documentElement)
      : null;
  const read = (name, fallback) =>
    style ? style.getPropertyValue(name).trim() || fallback : fallback;
  return {
    accent: read("--accent", "#ff2323"),
    bad: read("--bad", "#ff4d4d"),
    background: read("--black", "#000000"),
    dataFont: read("--font-data", "ui-monospace, Menlo, monospace"),
    good: read("--good", "#2fe36a"),
    hover: read("--matrix-hover", "#245663"),
    line: read("--line", "#242424"),
    lineStrong: read("--line-strong", "#3d3d3d"),
    muted: read("--muted", "#b3bcc4"),
    panel: read("--panel", "#0a0a0a"),
    cell: read("--raised", "#2b2f32"),
    statusGlyph: read("--status-glyph", "#000000"),
    text: read("--text", "#ffffff"),
    uiFont: read("--font-ui", "system-ui, sans-serif"),
    warn: read("--warn", "#ffc400"),
  };
}

function rowLabelText(row) {
  if (row.kind === "group") return row.name;
  if (row.kind === "device") return row.label;
  return row.name;
}

function rowLabelX(row) {
  if (row.kind === "device") return CELL + GUTTER_PADDING;
  return (
    CELL +
    GUTTER_PADDING +
    (row.grouped ? INDENT * 2 : INDENT) +
    (row.kind === "group" ? 18 : 0)
  );
}

function rowLabelFont(row, theme) {
  return `${row.kind === "device" ? "600 " : ""}13px ${theme.uiFont}`;
}

function columnLabelText(column) {
  if (column.kind === "group") return column.name;
  if (column.kind === "device") return column.label;
  return column.name;
}

export function measureMatrixLayout(context, rows, columns, theme) {
  let gutter = MINIMUM_GUTTER;
  for (const row of rows) {
    context.font = rowLabelFont(row, theme);
    const label = context.measureText(rowLabelText(row)).width;
    gutter = Math.max(gutter, rowLabelX(row) + label + GUTTER_PADDING + CELL);
  }
  context.font = `13px ${theme.uiFont}`;
  let header =
    rows.some((entry) => entry.kind === "group") ||
    columns.some((entry) => entry.kind === "group")
      ? 192
      : MINIMUM_HEADER;
  for (const column of columns) {
    context.font = `${column.kind === "device" ? "600 " : ""}13px ${theme.uiFont}`;
    header = Math.max(
      header,
      CELL * 2 +
        HEADER_PADDING +
        context.measureText(columnLabelText(column)).width,
    );
  }
  return { gutter: Math.ceil(gutter), header: Math.ceil(header) };
}

export function draw(
  context,
  {
    columns,
    flipped,
    hover,
    layout,
    pending,
    rows,
    scroll,
    size,
    subscriptionIndex,
    theme,
  },
) {
  const { gutter, header } = layout;
  context.clearRect(0, 0, size.width, size.height);
  context.fillStyle = theme.background;
  context.fillRect(0, 0, size.width, size.height);

  const firstColumn = Math.max(0, Math.floor(scroll.left / CELL));
  const lastColumn = Math.min(
    columns.length,
    Math.ceil((scroll.left + size.width - gutter) / CELL) + 1,
  );
  const firstRow = Math.max(0, Math.floor(scroll.top / CELL));
  const lastRow = Math.min(
    rows.length,
    Math.ceil((scroll.top + size.height - header) / CELL) + 1,
  );
  const columnX = (index) => gutter + index * CELL - scroll.left;
  const rowY = (index) => header + index * CELL - scroll.top;

  context.save();
  context.beginPath();
  context.rect(
    gutter,
    header,
    Math.max(
      0,
      Math.min(size.width - gutter, columns.length * CELL - scroll.left),
    ),
    Math.max(
      0,
      Math.min(size.height - header, rows.length * CELL - scroll.top),
    ),
  );
  context.clip();

  context.fillStyle = theme.cell;
  context.fillRect(gutter, header, size.width - gutter, size.height - header);
  for (let index = firstRow; index < lastRow; index += 1) {
    if (rows[index].kind !== "channel") {
      context.fillStyle = theme.panel;
      context.fillRect(gutter, rowY(index), size.width - gutter, CELL);
    }
  }
  for (let index = firstColumn; index < lastColumn; index += 1) {
    if (columns[index] && columns[index].kind !== "channel") {
      context.fillStyle = theme.panel;
      context.fillRect(columnX(index), header, CELL, size.height - header);
    }
  }

  if (hover) {
    context.fillStyle = theme.hover;
    if (!hover.inHeader && rows[hover.rowIndex]) {
      context.fillRect(gutter, rowY(hover.rowIndex), size.width - gutter, CELL);
    }
    if (!hover.inGutter && columns[hover.columnIndex]) {
      context.fillRect(
        columnX(hover.columnIndex),
        header,
        CELL,
        size.height - header,
      );
    }
  }

  context.lineWidth = 4;
  context.beginPath();
  context.strokeStyle = theme.background;
  for (let index = firstColumn; index <= lastColumn; index += 1) {
    const x = Math.floor(columnX(index));
    context.moveTo(x, header);
    context.lineTo(x, size.height);
  }
  for (let index = firstRow; index <= lastRow; index += 1) {
    const y = Math.floor(rowY(index));
    context.moveTo(gutter, y);
    context.lineTo(size.width, y);
  }
  context.stroke();

  context.lineWidth = 1;

  context.textAlign = "center";
  context.textBaseline = "middle";
  context.font = `600 10px ${theme.uiFont}`;
  for (let rowIndex = firstRow; rowIndex < lastRow; rowIndex += 1) {
    const row = rows[rowIndex];
    for (
      let columnIndex = firstColumn;
      columnIndex < lastColumn;
      columnIndex += 1
    ) {
      const column = columns[columnIndex];
      if (!column) {
        continue;
      }
      if (row.kind !== "channel" || column.kind !== "channel") {
        if (row.kind !== "device" || column.kind !== "device") continue;
        const entries = [row, column].filter(
          (entry) => entry.kind !== "channel",
        );
        const expand = entries.some((entry) => !entry.expanded);
        const x = columnX(columnIndex) + CELL / 2,
          y = rowY(rowIndex) + CELL / 2;
        context.strokeStyle = theme.muted;
        context.lineWidth = 1.5;
        context.beginPath();
        context.moveTo(x - 5, y);
        context.lineTo(x + 5, y);
        if (expand) {
          context.moveTo(x, y - 5);
          context.lineTo(x, y + 5);
        }
        context.stroke();
        context.lineWidth = 1;
        continue;
      }
      const state = cellState(row, column, subscriptionIndex, pending, flipped);
      const x = columnX(columnIndex);
      const y = rowY(rowIndex);
      if (
        state.kind === "self-unsupported" ||
        state.kind === "self-unavailable"
      ) {
        context.fillStyle = theme.panel;
        context.fillRect(
          Math.floor(x) + 2,
          Math.floor(y) + 2,
          CELL - 4,
          CELL - 4,
        );
        continue;
      }
      if (state.kind === "empty") {
        continue;
      }
      if (state.kind === "pending") {
        drawStatusIcon(context, x + CELL / 2, y + CELL / 2, "pending", theme);
        continue;
      }
      const severity =
        state.kind === "aggregate"
          ? state.severity
          : state.kind === "partial"
            ? severityName(
                severityRank(format.subscriptionTone(state.subscription)),
              )
            : state.kind;
      drawStatusIcon(context, x + CELL / 2, y + CELL / 2, severity, theme);
    }
  }
  context.restore();

  drawGutter(context, {
    firstRow,
    gutter,
    header,
    hover,
    rows,
    scroll,
    size,
    theme,
  });
  drawHeader(context, {
    columns,
    firstColumn,
    gutter,
    header,
    hover,
    lastColumn,
    scroll,
    size,
    theme,
  });

  context.fillStyle = theme.panel;
  context.fillRect(0, 0, gutter, header);
  context.strokeStyle = theme.background;
  context.lineWidth = 4;
  context.beginPath();
  context.moveTo(0, header);
  context.lineTo(
    Math.min(size.width, gutter + columns.length * CELL - scroll.left),
    header,
  );
  context.moveTo(gutter, 0);
  context.lineTo(
    gutter,
    Math.min(size.height, header + rows.length * CELL - scroll.top),
  );
  context.stroke();
  context.lineWidth = 1;
}

function drawExpansionMark(context, x, y, expanded, theme) {
  context.strokeStyle = theme.text;
  context.lineWidth = 1;
  context.strokeRect(x - 6, y - 6, 12, 12);
  context.beginPath();
  context.moveTo(x - 3, y);
  context.lineTo(x + 3, y);
  if (!expanded) {
    context.moveTo(x, y - 3);
    context.lineTo(x, y + 3);
  }
  context.stroke();
}

function severityColor(theme, severity) {
  if (severity === "error" || severity === "critical" || severity === "bad") {
    return theme.bad;
  }
  if (severity === "warning" || severity === "warn") {
    return theme.warn;
  }
  return theme.good;
}

function drawStatusIcon(context, x, y, severity, theme, size = CELL - 4) {
  context.save();
  context.translate(x - size / 2, y - size / 2);
  context.scale(size / 26, size / 26);
  context.fillStyle = severityColor(theme, severity);
  context.strokeStyle = theme.statusGlyph;
  context.lineWidth = 1.25;
  if (severity === "pending") {
    context.fillStyle = "#a6abad";
    context.fillRect(0, 0, 26, 26);
    context.strokeStyle = "#000000";
    context.lineWidth = 1;
    context.beginPath();
    context.moveTo(5, 4);
    context.lineTo(21, 4);
    context.moveTo(5, 22);
    context.lineTo(21, 22);
    context.moveTo(7, 4);
    context.bezierCurveTo(7, 10, 9, 11, 13, 13);
    context.bezierCurveTo(17, 15, 19, 16, 19, 22);
    context.moveTo(19, 4);
    context.bezierCurveTo(19, 10, 17, 11, 13, 13);
    context.bezierCurveTo(9, 15, 7, 16, 7, 22);
  } else if (severity === "warning") {
    context.fillRect(0, 0, 26, 26);
    context.beginPath();
    context.moveTo(13, 4);
    context.lineTo(22, 21);
    context.lineTo(4, 21);
    context.closePath();
    context.stroke();
    context.beginPath();
    context.moveTo(13, 9);
    context.lineTo(13, 15);
    context.moveTo(13, 17);
    context.lineTo(13, 19);
  } else if (severity === "error") {
    context.fillRect(0, 0, 26, 26);
    context.beginPath();
    context.arc(13, 13, 9, 0, Math.PI * 2);
    context.moveTo(6.6, 19.4);
    context.lineTo(19.4, 6.6);
  } else {
    context.fillRect(0, 0, 26, 26);
    context.beginPath();
    context.moveTo(6, 13);
    context.lineTo(11, 18);
    context.lineTo(21, 7);
  }
  context.stroke();
  context.restore();
}

function drawGutter(
  context,
  { firstRow, gutter, header, hover, rows, scroll, size, theme },
) {
  context.save();
  context.beginPath();
  context.rect(
    0,
    header,
    gutter,
    Math.max(
      0,
      Math.min(size.height - header, rows.length * CELL - scroll.top),
    ),
  );
  context.clip();
  context.fillStyle = theme.panel;
  context.fillRect(0, header, gutter, size.height - header);
  context.textBaseline = "middle";
  const lastRow = Math.min(
    rows.length,
    Math.ceil((scroll.top + size.height - header) / CELL) + 1,
  );
  for (let index = firstRow; index < lastRow; index += 1) {
    drawGutterRow(
      context,
      rows[index],
      index,
      header + index * CELL - scroll.top,
      { gutter, hover, theme },
    );
  }
  const pinned = stickyDevice(rows, scroll.top);
  if (pinned)
    drawGutterRow(
      context,
      rows[pinned.index],
      pinned.index,
      header + pinned.position,
      { gutter, hover, theme },
    );
  context.restore();
}

function drawGutterRow(context, row, index, y, { gutter, hover, theme }) {
  if (row.kind === "device") {
    context.fillStyle = theme.line;
    context.fillRect(0, y, gutter, CELL);
  }
  if (hover && hover.rowIndex === index && !hover.inHeader) {
    context.fillStyle = theme.hover;
    context.fillRect(0, y, gutter, CELL);
  }
  context.fillStyle = theme.background;
  context.fillRect(0, Math.floor(y), gutter, 2);
  context.fillRect(0, Math.floor(y) + CELL - 2, gutter, 2);
  context.fillStyle = theme.text;
  context.textAlign = "left";
  if (row.activity?.count) {
    drawStatusIcon(
      context,
      gutter - CELL / 2,
      y + CELL / 2,
      row.activity.severity,
      theme,
      18,
    );
    context.fillStyle = theme.text;
  }
  if (row.kind === "device") {
    drawExpansionMark(context, CELL / 2, y + CELL / 2, row.expanded, theme);
    context.font = rowLabelFont(row, theme);
    context.fillText(rowLabelText(row), rowLabelX(row), y + CELL / 2);
    if (row.device?.online === false)
      strikeLabel(context, rowLabelText(row), rowLabelX(row), y + CELL / 2);
  } else {
    context.font = rowLabelFont(row, theme);
    const x = CELL + GUTTER_PADDING + (row.grouped ? INDENT * 2 : INDENT);
    context.strokeStyle = theme.lineStrong;
    context.beginPath();
    context.moveTo(x - 16, y);
    context.lineTo(x - 16, y + CELL / 2);
    context.lineTo(x - 6, y + CELL / 2);
    context.stroke();
    if (row.kind === "group")
      drawExpansionMark(context, x + 6, y + CELL / 2, row.expanded, theme);
    context.fillText(rowLabelText(row), rowLabelX(row), y + CELL / 2);
  }
}

function drawHeader(
  context,
  {
    columns,
    firstColumn,
    gutter,
    header,
    hover,
    lastColumn,
    scroll,
    size,
    theme,
  },
) {
  context.save();
  context.beginPath();
  context.rect(
    gutter,
    0,
    Math.max(
      0,
      Math.min(size.width - gutter, columns.length * CELL - scroll.left),
    ),
    header,
  );
  context.clip();
  context.fillStyle = theme.panel;
  context.fillRect(gutter, 0, size.width - gutter, header);
  for (let index = firstColumn; index < lastColumn; index += 1) {
    if (columns[index])
      drawHeaderColumn(
        context,
        columns[index],
        index,
        gutter + index * CELL - scroll.left,
        { header, hover, theme },
      );
  }
  const pinned = stickyDevice(columns, scroll.left);
  if (pinned)
    drawHeaderColumn(
      context,
      columns[pinned.index],
      pinned.index,
      gutter + pinned.position,
      { header, hover, theme },
    );
  context.restore();
}

function drawHeaderColumn(context, column, index, x, { header, hover, theme }) {
  if (column.kind === "device") {
    context.fillStyle = theme.line;
    context.fillRect(x, 0, CELL, header);
  }
  if (hover && hover.columnIndex === index && !hover.inGutter) {
    context.fillStyle = theme.hover;
    context.fillRect(x, 0, CELL, header);
  }
  context.fillStyle = theme.background;
  context.fillRect(Math.floor(x), 0, 2, header);
  context.fillRect(Math.floor(x) + CELL - 2, 0, 2, header);
  if (column.kind === "device") {
    drawExpansionMark(context, x + CELL / 2, 14, column.expanded, theme);
  } else if (column.kind === "group") {
    context.font = `13px ${theme.uiFont}`;
    drawExpansionMark(
      context,
      x + CELL / 2,
      header -
        HEADER_PADDING -
        CELL -
        context.measureText(columnLabelText(column)).width -
        12,
      column.expanded,
      theme,
    );
  }
  context.save();
  if (column.activity?.count) {
    drawStatusIcon(
      context,
      x + CELL / 2,
      header - CELL / 2,
      column.activity.severity,
      theme,
    );
  }
  context.translate(x + CELL / 2, header - HEADER_PADDING - CELL);
  context.rotate(-Math.PI / 2);
  context.textAlign = "left";
  context.textBaseline = "middle";
  context.fillStyle = theme.text;
  context.font =
    column.kind === "device"
      ? `600 13px ${theme.uiFont}`
      : `13px ${theme.uiFont}`;
  context.fillText(columnLabelText(column), 0, 0);
  if (column.kind === "device" && column.device?.online === false)
    strikeLabel(context, columnLabelText(column), 0, 0);
  context.restore();
}

function strikeLabel(context, label, x, y) {
  context.fillRect(x, Math.round(y), context.measureText(label).width, 2);
}
