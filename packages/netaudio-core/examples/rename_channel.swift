import Foundation

let arguments = CommandLine.arguments
guard arguments.count == 4 else {
    FileHandle.standardError.write(Data("usage: \(arguments[0]) <device_ip> <tx_channel_number> <new_name>\n".utf8))
    exit(2)
}

print("netaudio-core ABI \(netaudio_abi_version())")

var client: OpaquePointer? = nil
var status = netaudio_client_new(arguments[1], 4440, 1000, 3, &client)
guard status == NETAUDIO_STATUS_OK else {
    FileHandle.standardError.write(Data("client_new failed: \(status.rawValue)\n".utf8))
    exit(1)
}

var buffer = [UInt8](repeating: 0, count: 65536)
var length: UInt = 0

status = netaudio_client_get_device_name_json(client, &buffer, UInt(buffer.count), &length)
if status == NETAUDIO_STATUS_OK {
    let deviceName = String(decoding: buffer[..<Int(length)], as: UTF8.self)
    print("device name: \(deviceName)")
}

let spec = """
{"command": "set_channel_name", "channel_type": "tx", "channel_number": \(arguments[2]), "name": "\(arguments[3])"}
"""

length = 0
status = netaudio_client_execute(client, spec, &buffer, UInt(buffer.count), &length)
netaudio_client_free(client)

guard status == NETAUDIO_STATUS_OK else {
    FileHandle.standardError.write(Data("set_channel_name failed: \(status.rawValue)\n".utf8))
    exit(1)
}

print("renamed tx channel \(arguments[2]) to \"\(arguments[3])\"")
