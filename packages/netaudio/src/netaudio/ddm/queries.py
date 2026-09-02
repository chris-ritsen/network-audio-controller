from __future__ import annotations

import textwrap


INVENTORY_OPERATION_NAME = "NetAudioManagedInventory"

INVENTORY_QUERY = textwrap.dedent(
    """
    query NetAudioManagedInventory {
      domains {
        id
        name
        status { summary clocking connectivity latency subscriptions }
        devices { ...ManagedDeviceFields }
      }
      unenrolledDevices { ...ManagedDeviceFields }
    }

    fragment ManagedDeviceFields on Device {
      id
      name
      domainId
      type
      enrolmentState
      identity {
        id
        instanceId
        defaultName
        actualName
        productModelId
        productModelName
        productVersion
        productSoftwareVersion
        danteVersion
        danteHardwareVersion
      }
      manufacturer { id name }
      platform { id name platformId }
      product { id name }
      interfaces { id macAddress address netmask subnet }
      connection { id state lastChanged }
      clockPreferences {
        id
        externalWordClock
        leader
        unicastClocking
        v1UnicastDelayRequests
      }
      capabilities {
        id
        CAN_WRITE_PREFERRED_MASTER
        CAN_WRITE_EXT_WORD_CLOCK
        CAN_WRITE_SLAVE_ONLY
        CAN_WRITE_UNICAST_DELAY_REQUESTS
        CAN_UNICAST_CLOCKING
        DDM_V_1_1_CLOCK_MESSAGES_SUPPORTED
        CAN_ENCRYPT_MEDIA
        CAN_RESET
        RTP_AUDIO_SUPPORTED
        RTP_AUDIO_SUPPORT_SUPPRESSED
        mediaTypes
      }
      clockingState {
        id
        locked
        grandLeader
        followerWithoutLeader
        multicastLeader
        unicastLeader
        unicastFollower
        muteStatus
        frequencyOffset
      }
      status {
        id
        summary
        clocking
        connectivity
        latency
        subscriptions
        alertMessage {
          id
          connectivity
          clocking
          latency
          subscriptions
        }
      }
      rxChannels {
        id
        index
        enabled
        name
        subscribedDevice
        subscribedChannel
        status
        statusMessage
        summary
        mediaType
        encryptionScheme
        canSubscribeSelf
        signalPresence { id leveldBFS status }
      }
      txChannels {
        id
        index
        name
        mediaType
        encryptionPolicy
        signalPresence { id leveldBFS status }
      }
      parameters { ...ManagedParameterFields }
      inputs {
        __typename
        id
        key
        parameters { ...ManagedParameterFields }
      }
      outputs {
        __typename
        id
        key
        parameters { ...ManagedParameterFields }
      }
    }

    fragment ManagedParameterFields on DeviceParameter {
      __typename
      id
      path
      key
      value
      label
      settable
      defaultValue
      units
      applyMode
      group
      renderingHint
      ... on DeviceParameterDiscrete { options }
      ... on DeviceParameterRanged { min max precision }
      ... on DeviceParameterStringWithRegex { regex }
    }
    """
).strip()


__all__ = ["INVENTORY_OPERATION_NAME", "INVENTORY_QUERY"]
