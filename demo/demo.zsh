#!/usr/bin/zsh

clear

echo netaudio device list
netaudio device list

echo ''
echo 'netaudio channel list --device-name=avio-output-2'
netaudio channel list --device-name=avio-output-2

echo ''
echo 'netaudio channel list --device-name=avio-usb-2'
netaudio channel list --device-name=avio-usb-2

echo ''
echo 'netaudio subscription list | head -n15 | sort -R | head -n10 | sort'
netaudio subscription list | head -n15 | sort -R | head -n10 | sort

echo ''
echo "netaudio subscription remove --rx-channel-name=128 --rx-device-name=lx-dante"
netaudio subscription remove --rx-channel-name=128 --rx-device-name=lx-dante

echo ''
echo "netaudio subscription add --tx-device-name='lx-dante' --tx-channel-name='128' --rx-channel-name='128' --rx-device-name='lx-dante'"
netaudio subscription add --tx-device-name='lx-dante' --tx-channel-name='128' --rx-channel-name='128' --rx-device-name='lx-dante'

echo ''
echo "netaudio config --set-device-name='DI Box' --device-host='dinet-tx-1'"
netaudio config --set-device-name='DI Box' --device-host='dinet-tx-1'

echo ''
echo "netaudio device list | grep -i 'DI Box'"
netaudio device list | grep -i 'DI Box'

echo ''
echo "netaudio config --set-device-name='dinet-tx-1' --device-host='192.168.1.41'"
netaudio config --set-device-name='dinet-tx-1' --device-host='192.168.1.41'

echo ''
echo "netaudio device list | grep -i 'dinet-tx'"
netaudio device list | grep -i 'dinet-tx'

netaudio device list --json | underscore map 'value' | underscore pluck ipv4 --outfmt text | sort
