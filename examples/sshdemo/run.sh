#!/bin/sh
# Runs inside alpine:3.22 with linux binaries of resumux and flakyproxy in /d.
# See examples/sshdemo/README.md.
set -e
apk add -q openssh >/dev/null
ssh-keygen -A >/dev/null
adduser -D demo && echo 'demo:unused-password' | chpasswd >/dev/null 2>&1
ssh-keygen -q -t ed25519 -N "" -f /root/k
mkdir -p /home/demo/.ssh && cp /root/k.pub /home/demo/.ssh/authorized_keys
chown -R demo /home/demo/.ssh && chmod 700 /home/demo/.ssh && chmod 600 /home/demo/.ssh/authorized_keys
/usr/sbin/sshd
head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > /token
cd /d
./resumux server -listen 127.0.0.1:8080 -target 127.0.0.1:22 -token-file /token 2>/srv.log &
./flakyproxy -listen 127.0.0.1:9000 -upstream 127.0.0.1:8080 -cut-every 3s 2>/flaky.log &
./flakyproxy -listen 127.0.0.1:9022 -upstream 127.0.0.1:22 -cut-every 3s 2>/flaky-direct.log &
./resumux client -listen 127.0.0.1:2222 -server ws://127.0.0.1:9000/ -token-file /token 2>/cli.log &
sleep 1
SSH="ssh -i /root/k -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
CMD='for i in $(seq 1 20); do echo "tick $i $(date +%T)"; sleep 1; done'
echo "===== SSH without resumux (flakyproxy straight in front of port 22)"
$SSH -p 9022 demo@127.0.0.1 "$CMD" || echo ">>> SSH was cut off (exit $?)"
echo "===== SSH with resumux (flakyproxy between client and server)"
$SSH -p 2222 demo@127.0.0.1 "$CMD" && echo ">>> the SSH session lasted all 20 seconds"
echo "===== flakyproxy log (resumux path)"; cat /flaky.log
