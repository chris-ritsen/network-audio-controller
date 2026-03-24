import xml.etree.ElementTree as ET
from xml.dom import minidom
import binascii


class DantePresetXMLSerializer:
    @staticmethod
    def devices_to_xml(
        devices_data, preset_name="dante", description="Dante Controller preset"
    ):
        root = ET.Element("preset", version="2.1.0")
        name_elem = ET.SubElement(root, "name")
        name_elem.text = preset_name
        desc_elem = ET.SubElement(root, "description")
        desc_elem.text = description

        for server_name, device_data in devices_data.items():
            device_elem = ET.SubElement(root, "device")
            DantePresetXMLSerializer._serialize_device(device_elem, device_data)

        return DantePresetXMLSerializer._prettify_xml(root)

    @staticmethod
    def _serialize_device(device_elem, device_data):
        name_elem = ET.SubElement(device_elem, "name")
        name_elem.text = device_data.get("name", "")
        server_name = device_data.get("server_name", "")

        if server_name:
            default_name = server_name.replace(".local.", "").upper()
        else:
            default_name = device_data.get("name", "").upper()

        default_name_elem = ET.SubElement(device_elem, "default_name")
        default_name_elem.text = default_name
        instance_id_elem = ET.SubElement(device_elem, "instance_id")
        services = device_data.get("services", {})
        device_id = None

        for service_name, service_data in services.items():
            properties = service_data.get("properties", {})

            if "id" in properties:
                device_id = properties["id"].upper()
                break

        if device_id:
            device_id_elem = ET.SubElement(instance_id_elem, "device_id")
            device_id_elem.text = device_id

        process_id_elem = ET.SubElement(instance_id_elem, "process_id")
        process_id_elem.text = "0"

        manufacturer_name = None

        for service_name, service_data in services.items():
            properties = service_data.get("properties", {})

            if "mf" in properties:
                manufacturer_name = properties["mf"]
                break

        if manufacturer_name:
            mfr_name_elem = ET.SubElement(device_elem, "manufacturer_name")
            mfr_name_elem.text = manufacturer_name

        model_id = device_data.get("model_id", "")

        if model_id:
            if len(model_id) < 16:
                model_id_hex = binascii.hexlify(model_id.encode()).decode().upper()
                model_id_hex = model_id_hex.ljust(16, "0")
            else:
                model_id_hex = model_id

            model_id_elem = ET.SubElement(device_elem, "model_id")
            model_id_elem.text = model_id_hex

            device_type_elem = ET.SubElement(device_elem, "device_type")
            device_type_elem.text = model_id_hex

            device_type_string_elem = ET.SubElement(device_elem, "device_type_string")
            device_type_string_elem.text = model_id

        friendly_name_elem = ET.SubElement(device_elem, "friendly_name")
        friendly_name_elem.text = device_data.get("name", "")

        preferred_master = device_data.get("preferred_master")

        if preferred_master is not None:
            preferred_master_elem = ET.SubElement(
                device_elem, "preferred_master", value=str(preferred_master).lower()
            )

        sample_rate = device_data.get("sample_rate")

        if sample_rate:
            samplerate_elem = ET.SubElement(device_elem, "samplerate")
            samplerate_elem.text = str(sample_rate)

        encoding = device_data.get("encoding")

        if encoding:
            encoding_elem = ET.SubElement(device_elem, "encoding")
            encoding_elem.text = str(encoding)

        latency = device_data.get("unicast_latency") or device_data.get("latency")

        if latency:
            unicast_latency_elem = ET.SubElement(device_elem, "unicast_latency")
            unicast_latency_elem.text = str(latency)

        channels = device_data.get("channels", {})
        transmitters = channels.get("transmitters", {})

        for channel_num, channel_data in transmitters.items():
            tx_elem = ET.SubElement(
                device_elem, "txchannel", danteId=str(channel_num), mediaType="audio"
            )
            label_elem = ET.SubElement(tx_elem, "label")
            label_elem.text = channel_data.get("friendly_name") or channel_data.get(
                "name", f"tx-{channel_num}"
            )

        receivers = channels.get("receivers", {})
        subscriptions = device_data.get("subscriptions", [])

        for channel_num, channel_data in receivers.items():
            rx_elem = ET.SubElement(
                device_elem, "rxchannel", danteId=str(channel_num), mediaType="audio"
            )

            name_elem = ET.SubElement(rx_elem, "name")
            name_elem.text = channel_data.get("name", f"rx-{channel_num}")

            for sub in subscriptions:
                if (
                    sub.get("rx_channel") == channel_data.get("name")
                    and sub.get("tx_device") is not None
                ):
                    sub_channel_elem = ET.SubElement(rx_elem, "subscribed_channel")
                    sub_channel_elem.text = sub.get("tx_channel", "")

                    sub_device_elem = ET.SubElement(rx_elem, "subscribed_device")
                    tx_device = sub.get("tx_device", "")
                    rx_device = sub.get("rx_device", "")

                    if tx_device == rx_device:
                        sub_device_elem.text = "."
                    else:
                        sub_device_elem.text = tx_device
                    break

    @staticmethod
    def _prettify_xml(elem):
        rough_string = ET.tostring(elem, encoding="unicode")
        reparsed = minidom.parseString(rough_string)
        pretty = reparsed.toprettyxml(indent="    ", encoding="UTF-8")

        lines = [line for line in pretty.decode("utf-8").split("\n") if line.strip()]

        lines[0] = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        return "\n".join(lines)
