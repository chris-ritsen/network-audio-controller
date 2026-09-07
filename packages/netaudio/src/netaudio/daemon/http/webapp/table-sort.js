const collator = new Intl.Collator(undefined, { numeric: true, sensitivity: "base" });

function cellValue(cell) {
  if (cell === null || cell === undefined) return null;
  if (typeof cell !== "object") return cell;
  if (Array.isArray(cell)) return cell.map(cellValue).filter((value) => value !== null).join(" ");
  const props = cell.props || {};
  if ("value" in props) return cellValue(props.value);
  if ("online" in props) return props.online ? "online" : "offline";
  return cellValue(props.children);
}

function missing(value) {
  return value === null || value === undefined || value === "" || value === "—";
}

export function sortRows(rows, column, direction) {
  if (!column) return rows;
  const valueOf = column.sortValue || ((row) => cellValue(column.cell(row)));
  return rows.map((row, index) => ({ row, index, value: valueOf(row) })).sort((a, b) => {
    if (missing(a.value) || missing(b.value)) {
      return Number(missing(a.value)) - Number(missing(b.value)) || a.index - b.index;
    }
    const comparison = typeof a.value === "number" && typeof b.value === "number"
      ? a.value - b.value
      : collator.compare(String(a.value), String(b.value));
    return (direction === "descending" ? -comparison : comparison) || a.index - b.index;
  }).map(({ row }) => row);
}
