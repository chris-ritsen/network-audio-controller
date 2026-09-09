const reportedBoolean = (value) => typeof value === "boolean" ? value : null;

export function aes67Status(device) {
  const supported = reportedBoolean(device.aes67_supported);
  const current = reportedBoolean(device.aes67_current);
  const configured = reportedBoolean(device.aes67_configured);
  const managed = device.ddm_enrolment_state === "ENROLLED" && Boolean(device.ddm_domain_id);
  const status = { supported, current, configured, managed, pending: false, canConfigure: !managed && supported === true };
  if (managed) {
    const capabilities = device.ddm_capabilities;
    return {
      ...status,
      label: "Managed by DDM",
      rtpAvailable: reportedBoolean(capabilities?.rtp_audio_supported),
      rebootRequired: reportedBoolean(capabilities?.rtp_audio_support_suppressed),
    };
  }
  if (supported === false) {
    return { ...status, label: "Unsupported" };
  }
  if (current !== null && configured !== null && current !== configured) {
    return {
      ...status,
      pending: true,
      label: configured ? "Enable pending" : "Disable pending",
    };
  }
  if (current !== null) return { ...status, label: current ? "Enabled" : "Disabled" };
  if (configured !== null) {
    return {
      ...status,
      label: configured ? "Configured enabled" : "Configured disabled",
    };
  }
  if (supported === true) {
    return { ...status, label: "Supported, state unknown" };
  }
  return { ...status, label: "Not reported" };
}
