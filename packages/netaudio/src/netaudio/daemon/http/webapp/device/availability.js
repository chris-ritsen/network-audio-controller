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
  const availability = operationAvailability(device, operation);
  return availability?.writable === true || availability?.write_permitted === true;
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
