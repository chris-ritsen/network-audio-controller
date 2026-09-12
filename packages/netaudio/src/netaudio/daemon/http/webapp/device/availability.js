const REASON_TEXT = {
  capability_unknown: "Capability support was not reported.",
  unsupported: "The device reports that this operation is unsupported.",
  read_only: "The device reports that this setting is read-only.",
  fixed: "The device reports that this setting is fixed.",
  update_mode_unknown: "The setting's update mode was not reported.",
  value_not_advertised: "The requested value was not advertised by the device.",
  host_disabled: "The host has disabled this setting.",
  no_device_adapter:
    "No supported control adapter is available for this device.",
  device_locked: "The device is locked.",
  lock_state_unknown: "The device lock state was not reported.",
  managed_permission_missing:
    "Permission for this managed operation was not configured.",
  managed_permission_denied:
    "Permission for this managed operation was denied.",
  managed_transport_unavailable:
    "This operation is unavailable through managed control.",
  protocol_unknown: "The device ARC protocol was not reported.",
  protocol_unsupported:
    "The device ARC protocol does not support this operation.",
  property_directory_unknown: "The device property directory was not reported.",
  properties_not_advertised:
    "The device does not advertise the required properties.",
  software_version_unknown:
    "The device software version was not reported in x.y.z form.",
  compatibility_property_not_advertised:
    "The device does not advertise the compatibility property required by its software version.",
};

export function operationAvailability(device, operation) {
  const availability = device?.operation_availability?.[operation];
  if (
    !availability ||
    typeof availability !== "object" ||
    Array.isArray(availability)
  )
    return null;
  return availability;
}

export function operationWritable(device, operation) {
  return operationAvailability(device, operation)?.writable === true;
}

export function operationReasonText(device, operation) {
  const availability = operationAvailability(device, operation);
  if (!availability) return "Operation availability was not reported.";
  if (availability.writable === true) return "";
  const reasons = Array.isArray(availability.reasons)
    ? availability.reasons
    : [];
  if (!reasons.length) return "This operation is unavailable.";
  return reasons
    .map(
      (reason) =>
        REASON_TEXT[reason] ||
        "The device reported an unknown availability restriction.",
    )
    .join(" ");
}

export function performanceOperationAvailability(device, operation) {
  const availability = device?.performance_operation_availability?.[operation];
  if (
    !availability ||
    typeof availability !== "object" ||
    Array.isArray(availability)
  )
    return null;
  return availability;
}

export function performanceOperationWritable(device, operation) {
  return performanceOperationAvailability(device, operation)?.writable === true;
}

export function performanceOperationReasonText(device, operation) {
  const availability = performanceOperationAvailability(device, operation);
  if (!availability) return "Operation availability was not reported.";
  if (availability.writable === true) return "";
  const reasons = Array.isArray(availability.reasons)
    ? availability.reasons
    : [];
  if (!reasons.length) return "This operation is unavailable.";
  return reasons
    .map(
      (reason) =>
        REASON_TEXT[reason] || "The device reported an unknown restriction.",
    )
    .join(" ");
}
