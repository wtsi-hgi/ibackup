#!/bin/bash
#
# Starts and stops a throwaway iRODS 4.3.5 zone and wr manager for the tests,
# using the same server and client images as CI. See developers/README.md.
#
#   testzone.sh run CMD...  start a zone in a temp dir, run CMD with the
#                           test env set, then tear everything down
#   testzone.sh up DIR      start a zone, writing DIR/env.sh for bash to source
#   testzone.sh down DIR    tear down the zone started in DIR
#
# The server runs in Singularity or Apptainer, or in docker or podman if
# IBACKUP_TEST_RUNTIME says so. The clients (iCommands and baton-do) always run
# in Singularity or Apptainer, using the client image's wrappers, as in CI. Each
# zone uses its own block of ports, locked so that concurrent zones on a host
# don't collide, and its own wr manager.

set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
workflow=$here/../.github/workflows/tests.yml
cache=${XDG_CACHE_HOME:-$HOME/.cache}/ibackup/testzone

# Each zone gets ports base..base+259: the zone port, the control plane port,
# the wr manager and its web port, and the 200 ports from base+60 that the
# server offers for parallel transfers. Bases are below the usual ephemeral
# port range, so that outgoing connections don't take them.
block_size=260
first_base=21000
last_base=$((32768 - block_size))

# the user's settings that are unset, so that only the zone's own apply
# (configor, which wr uses to read its config, reads CONFIGOR_* too)
own_settings='^(IRODS_|WR_|CONFIGOR_|IBACKUP_TEST_COLLECTION)'
keep_settings='^IRODS_(SERVER|CLIENT)_IMAGE$'

die() {
	echo "testzone: $*" >&2
	exit 1
}

# image NAME prints the image CI uses, unless NAME is set in the environment.
image() {
	local ref=${!1:-}

	if [ -z "$ref" ]; then
		ref=$(sed -n "/^ *$1: */{s///p;q}" "$workflow")
	fi

	[ -n "$ref" ] || die "could not find $1 in $workflow"
	echo "$ref"
}

# unset_settings unsets the user's own iRODS, wr and test collection settings.
unset_settings() {
	local var

	while read -r var; do
		unset "$var"
	done < <(compgen -e | grep -E "$own_settings" | grep -Ev "$keep_settings")
}

# find_singularity puts singularity on PATH, loading a module for it if needed.
# The client wrappers call "singularity", so for Apptainer alone a link to it is
# made in DIR.
find_singularity() {
	local dir=$1

	if ! command -v singularity >/dev/null && ! command -v apptainer >/dev/null; then
		if ! type module >/dev/null 2>&1 && [ -r /etc/profile.d/modules.sh ]; then
			# shellcheck disable=SC1091
			source /etc/profile.d/modules.sh
		fi

		if type module >/dev/null 2>&1; then
			module load "${IBACKUP_TEST_SINGULARITY_MODULE:-ISG/singularity/3.11.4}" >/dev/null 2>&1 || true
		fi
	fi

	if ! command -v singularity >/dev/null; then
		command -v apptainer >/dev/null ||
			die "the iRODS clients need singularity or apptainer on PATH, or a module for it (set IBACKUP_TEST_SINGULARITY_MODULE)"

		mkdir -p "$dir/bin"
		ln -sf "$(command -v apptainer)" "$dir/bin/singularity"
		PATH=$dir/bin:$PATH
	fi

	singularity_dir=$(dirname "$(command -v singularity)")
}

# pick_runtime sets runtime to IBACKUP_TEST_RUNTIME, by default singularity.
pick_runtime() {
	runtime=${IBACKUP_TEST_RUNTIME:-singularity}

	case $runtime in
	singularity) ;;
	docker | podman)
		"$runtime" info >/dev/null 2>&1 || die "IBACKUP_TEST_RUNTIME is $runtime, but '$runtime info' fails"
		;;
	*) die "IBACKUP_TEST_RUNTIME must be singularity, docker or podman, not '$runtime'" ;;
	esac
}

# zone_settings DIR prints, as NAME=VALUE lines, every wr manager setting for
# DIR's zone: the manager's own and all those its clients read. wr env vars that
# aren't empty win over its config files, so with these set no config file can
# point the zone's manager or its clients anywhere else.
zone_settings() {
	local m=$1/wr_development

	printf '%s\n' WR_MANAGERHOST=localhost "WR_MANAGERPORT=$wr_port" "WR_MANAGERWEB=$wr_web" \
		"WR_MANAGERDIR=$1/wr" "WR_MANAGERTOKENFILE=$m/client.token" "WR_MANAGERCAFILE=$m/ca.pem" \
		WR_MANAGERCERTDOMAIN=localhost
}

# zone_wr DIR ARGS... runs wr ARGS for DIR's zone manager, with only the zone's
# wr settings. It runs in DIR with a HOME of its own, so that no user config
# file can reach the manager's other settings either, at start or at stop. (Once
# daemonized, the manager also reads /.wr_config*.yml, as its working dir
# becomes /, but only root could write those.)
zone_wr() {
	local dir=$1 settings
	shift
	mapfile -t settings < <(zone_settings "$dir")

	(
		unset_settings
		cd "$dir"
		env HOME="$dir/wrhome" "${settings[@]}" "$wr_bin" "$@" --deployment development </dev/null
	)
}

# lock_ports sets base to the first block of free ports that no other zone has
# locked, and starts a process that holds the lock until down.
lock_ports() {
	local dir=$1 lock fd

	for ((base = first_base; base <= last_base; base += block_size)); do
		lock=/tmp/ibackup-testzone-$base.lock

		# Other users' zones need to read, and so lock, the same file.
		(umask 022 && : 2>/dev/null >>"$lock") || true
		[ -r "$lock" ] || continue
		exec {fd}<"$lock"

		if flock -n "$fd" && ports_free "$base"; then
			setsid sleep infinity </dev/null >/dev/null 2>&1 &
			echo $! >"$dir/holder"
			exec {fd}<&-

			return
		fi

		exec {fd}<&-
	done

	die "no free block of $block_size ports between $first_base and 32768"
}

# ports_free BASE succeeds if no TCP socket on this host uses a port in BASE's
# block.
ports_free() {
	ss -Htan | awk -v lo="$1" -v hi=$(($1 + block_size - 1)) '
		{ n = split($4, a, ":"); p = a[n] + 0; if (p >= lo && p <= hi) bad = 1 }
		END { exit bad }'
}

# pull REF FILE pulls the docker image REF to the Singularity image FILE, unless
# it is already there. Singularity may not accept a tag and digest together, so
# a tag is dropped when there is a digest.
pull() {
	local ref=$1 file=$2

	[ -s "$file" ] && return

	ref=$(echo "$ref" | sed -E 's/:[^/@]+@/@/')
	singularity --silent pull -F "$file.part" "docker://$ref" >/dev/null ||
		die "could not pull $ref"
	mv "$file.part" "$file"
}

# digest REF prints the first 12 hex digits of REF's digest, or REF made safe
# for a file name if it has none.
digest() {
	case $1 in
	*@sha256:*) echo "${1##*@sha256:}" | cut -c 1-12 ;;
	*) echo "$1" | tr -c 'A-Za-z0-9.\n' _ ;;
	esac
}

# random prints 32 random letters and digits.
random() {
	local s=''

	while [ ${#s} -lt 32 ]; do
		s+=$(head -c 48 /dev/urandom | base64 | tr -dc 'A-Za-z0-9')
	done

	echo "${s:0:32}"
}

up() {
	local dir server_image client_image clients sif fd

	unset_settings

	if [ -e "$1" ] && [ -n "$(ls -A "$1")" ]; then
		die "$1 is not empty"
	fi

	# The zone's files include its passwords and keys.
	dir=$(mkdir -p "$1" && chmod 700 "$1" && cd "$1" && pwd)
	mkdir "$dir/irods"
	tear_down_on_exit "$dir"
	server_image=$(image IRODS_SERVER_IMAGE)
	client_image=$(image IRODS_CLIENT_IMAGE)
	wr_bin=$(command -v wr) ||
		die "wr is not on PATH; go install github.com/VertebrateResequencing/wr@$(image WR_VERSION)"
	wr_bin=$(realpath "$wr_bin")

	pick_runtime
	find_singularity "$dir"
	mkdir -p "$cache" || die "could not create the image cache $cache"
	lock_ports "$dir"

	local zone_port=$base ctrl_port=$((base + 1)) wr_port=$((base + 2)) wr_web=$((base + 3))
	local range_start=$((base + 60)) range_end=$((base + block_size - 1))
	local name admin_pw user_pw ctrl_key

	name=ibackup-testzone-$(id -un)-$base
	admin_pw=$(random)
	user_pw=$(random)
	ctrl_key=$(random)

	printf '%s=%q\n' runtime "$runtime" name "$name" singularity_dir "$singularity_dir" \
		wr_bin "$wr_bin" wr_port "$wr_port" wr_web "$wr_web" >"$dir/state"

	# A pull into the cache, which may be shared by hosts, is locked across them.
	exec {fd}>"$cache/pull.lock"
	flock -w 1800 "$fd" || die "timed out waiting for another zone to pull the images"

	clients=$cache/clients-$(digest "$client_image")
	sif=$cache/clients-$(digest "$client_image").sif
	pull "$client_image" "$sif"

	if [ ! -x "$clients/bin/baton-do" ]; then
		rm -rf "$clients.part" "$clients"
		singularity --silent exec "$sif" singularity-wrapper -s -p "$clients.part" install >/dev/null
		mv "$clients.part" "$clients"
	fi

	if [ "$runtime" = singularity ]; then
		pull "$server_image" "$cache/server-$(digest "$server_image").sif"
	else
		"$runtime" pull -q "$server_image" >/dev/null
	fi

	exec {fd}>&-

	start_client_instance "$clients"

	echo "testzone: starting iRODS with $runtime on ports $base-$range_end in $dir" >&2

	# The control plane key is replaced too, as the image's is well known.
	local config
	config=$(
		cat <<-EOF
			cd /etc/irods
			jq --argjson zp $zone_port --argjson cp $ctrl_port --argjson rs $range_start --argjson re $range_end --arg ck $ctrl_key \
				'.zone_port=\$zp | .server_control_plane_port=\$cp | .server_control_plane_key=\$ck | .server_port_range_start=\$rs | .server_port_range_end=\$re | .plugin_configuration.database.postgres.db_host="/var/run/postgresql"' \
				server_config.json >s.new && mv s.new server_config.json
			e=/var/lib/irods/.irods/irods_environment.json
			jq --argjson zp $zone_port --argjson cp $ctrl_port --arg ck $ctrl_key \
				'.irods_port=\$zp | .irods_server_control_plane_port=\$cp | .irods_server_control_plane_key=\$ck' \$e >\$e.new && mv \$e.new \$e
			# PostgreSQL only on its unix socket, so concurrent zones don't collide
			sed -i 's|^Servername=.*|Servername=/var/run/postgresql|' /var/lib/irods/.odbc.ini
			sed -i -E 's/^(local\s+all\s+all\s+)peer/\1md5/' /etc/postgresql/14/main/pg_hba.conf
		EOF
	)

	if [ "$runtime" = singularity ]; then
		start_singularity_server "$dir" "$name" "$config"
	else
		# The image runs as root, so its own start script can run as normal, once
		# the files that root rewrote are given back to irods. Unlike CI, the
		# container gets its own /dev/shm, as concurrent zones' servers might
		# clash in the host's.
		"$runtime" run -d --name "$name" --network host --shm-size 1g --entrypoint bash "$server_image" -c "
			set -e
			$config
			chown irods:irods /etc/irods/server_config.json \$e
			echo \"listen_addresses = ''\" >>/etc/postgresql/14/main/postgresql.conf
			exec /opt/docker/irods/scripts/start_irods.sh" >/dev/null
	fi

	wait_for_port "$zone_port" || fail "$dir" "the iRODS server did not start listening on port $zone_port"

	local ir=$dir/irods
	write_env "$ir" admin irods "$zone_port"
	write_env "$ir" user ibackup "$zone_port"
	PATH=$clients/bin:$PATH:$clients/bin

	# The server can take a while after it listens before it accepts logins.
	local i
	for i in $(seq 30); do
		if echo irods | client "$ir" admin iinit >/dev/null 2>"$dir/iinit.err"; then
			break
		fi

		if [ "$i" -eq 30 ]; then
			echo "testzone: the last iinit's errors:" >&2
			tail -n 20 "$dir/iinit.err" >&2
			fail "$dir" "could not log in to the iRODS server"
		fi

		sleep 1
	done

	# Replace the image's well-known admin password, since the zone port listens
	# on all interfaces. The passwords go to iadmin on stdin, not as arguments
	# that other users could see.
	client "$ir" admin iadmin >/dev/null <<<"moduser irods password $admin_pw"
	set_service_password "$name" "$admin_pw"
	echo "$admin_pw" | client "$ir" admin iinit >/dev/null
	client "$ir" admin iadmin mkuser ibackup rodsuser </dev/null
	client "$ir" admin iadmin >/dev/null <<<"moduser ibackup password $user_pw"
	echo "$user_pw" | client "$ir" user iinit >/dev/null

	mkdir -m 700 "$dir/wrhome"

	if ! zone_wr "$dir" manager start -s local >"$dir/wr-start.log" 2>&1; then
		tail -n 20 "$dir/wr-start.log" >&2
		die "could not start a wr manager"
	fi

	ports_listening "$wr_port" || die "the wr manager is not on port $wr_port"

	# The client bin dir is both first in PATH, so the tests use it for iCommands,
	# and last, as extendo's FindBaton returns the last baton-do in PATH.
	cat >"$dir/env.sh" <<-EOF
		while read -r var; do
			unset "\$var"
		done < <(compgen -e | grep -E '$own_settings' | grep -Ev '$keep_settings')
		unset var
		export PATH="$clients/bin:$singularity_dir:\$PATH:$clients/bin"
		export IRODS_ENVIRONMENT_FILE="$ir/user.json"
		export IBACKUP_TEST_IRODS_ADMIN_ENVIRONMENT_FILE="$ir/admin.json"
		export IBACKUP_TEST_COLLECTION=/testZone/home/ibackup/ibackup_testsuite
		export IBACKUP_TEST_SCHEDULER=development
		export WR_DEPLOYMENT=development
		$(zone_settings "$dir" | while IFS= read -r kv; do printf 'export %q\n' "$kv"; done)
	EOF
}

# start_singularity_server DIR NAME CONFIG starts the server in a writable
# sandbox of the image, as the invoking user rather than the image's irods and
# postgres users.
start_singularity_server() {
	local dir=$1 name=$2 config=$3 server_image

	server_image=$(image IRODS_SERVER_IMAGE)
	singularity --silent build -F --sandbox "$dir/server" "$cache/server-$(digest "$server_image").sif" >/dev/null 2>&1 ||
		die "could not make a sandbox of the server image"

	cat >"$dir/server/start.sh" <<-EOF
		#!/bin/bash
		set -euo pipefail
		$config
		rm -f /var/lib/postgresql/14/main/postmaster.pid
		/usr/lib/postgresql/14/bin/pg_ctl -D /var/lib/postgresql/14/main -w -l /var/lib/irods/log/postgres.log \
			-o "-c config_file=/etc/postgresql/14/main/postgresql.conf -c listen_addresses= -c ssl=off" start >/dev/null
		# .irodsA obfuscation is salted with the uid; re-encode it for ours
		python3 -c 'import sys; sys.path.insert(0, "/var/lib/irods/scripts"); from irods import password_obfuscation as p; open("/var/lib/irods/.irods/.irodsA", "w").write(p.encode("irods"))'
		export IRODS_ENABLE_TEST_MODE=1 HOME=/var/lib/irods ODBCINI=/var/lib/irods/.odbc.ini
		cd /var/lib/irods && ./irodsctl start --test >/dev/null
	EOF
	chmod +x "$dir/server/start.sh"

	singularity --silent instance start --writable --no-home --containall "$dir/server" "$name" </dev/null >/dev/null 2>&1 ||
		fail "$dir" "could not start the server instance"
	singularity --silent exec --cleanenv instance://"$name" /start.sh </dev/null >/dev/null ||
		fail "$dir" "could not start the iRODS server"
}

# start_client_instance CLIENTS starts the service instance that the client
# wrappers in CLIENTS use, unless it is running. Otherwise the first wrapper to
# run would start it, and leave a logger holding its caller's stdout open. The
# instance is shared with anything else using the same wrappers, so, as with
# the wrappers, nothing here stops it. Taking the wrappers' own lock stops this
# and a wrapper starting it at the same time. The instance must not inherit the
# lock, or it would hold it for as long as it runs, and every wrapper would
# fail to get it.
start_client_instance() {
	local clients=$1 instance fd

	# the wrappers' own name for it
	instance=$(sed -n 's/^export DOCKER_\(REGISTRY\|USER\|IMAGE\|TAG\)="\(.*\)"/\2/p' "$clients/bin/baton-do" |
		paste -sd ' ' | sed 's/ /--/g')

	[ -n "${USER:-}" ] || die "USER must be set, as the client wrappers need it"
	exec {fd}>"/var/lock/$instance--$USER.lock"
	flock -w 60 "$fd" || die "timed out waiting to start the iRODS client instance"

	if ! singularity --silent instance list | awk -v n="$instance" '$1 == n { f = 1 } END { exit !f }'; then
		singularity --silent instance start "$clients.sif" "$instance" </dev/null >/dev/null 2>&1 {fd}>&- ||
			die "could not start the iRODS client instance"
	fi

	flock -u "$fd"
	exec {fd}>&-
}

# set_service_password NAME PASSWORD re-encodes the server's own .irodsA in
# container NAME for its new admin PASSWORD.
set_service_password() {
	local name=$1 encode
	encode='import os, sys; sys.path.insert(0, "/var/lib/irods/scripts"); from irods import password_obfuscation as p; open("/var/lib/irods/.irods/.irodsA", "w").write(p.encode(os.environ["ADMIN_PW"]))'

	if [ "$runtime" = singularity ]; then
		SINGULARITYENV_ADMIN_PW=$2 APPTAINERENV_ADMIN_PW=$2 \
			singularity --silent exec --cleanenv instance://"$name" python3 -c "$encode"
	else
		ADMIN_PW=$2 "$runtime" exec -u irods -e ADMIN_PW "$name" python3 -c "$encode"
	fi
}

# write_env DIR NAME USER PORT writes the iRODS environment file DIR/NAME.json
# for USER.
write_env() {
	cat >"$1/$2.json" <<-EOF
		{
		    "irods_host": "localhost",
		    "irods_port": $4,
		    "irods_user_name": "$3",
		    "irods_zone_name": "testZone",
		    "irods_home": "/testZone/home/$3",
		    "irods_default_resource": "replResc",
		    "irods_default_hash_scheme": "MD5",
		    "irods_authentication_file": "$1/.irodsA-$3"
		}
	EOF
}

# client DIR NAME CMD... runs an iRODS client command as NAME, with a HOME of
# DIR so that the user's own ~/.irods is untouched. The rest of the environment
# is kept (the user's own iRODS settings are already unset), as Singularity can
# need it to run the wrappers' commands in their instance: for example, rootless
# SingularityCE 4 puts instances in cgroups through the user's DBus session.
client() {
	local dir=$1 name=$2
	shift 2

	HOME=$dir IRODS_ENVIRONMENT_FILE=$dir/$name.json "$@"
}

wait_for_port() {
	local i

	for i in $(seq 120); do
		if (exec 3<>"/dev/tcp/localhost/$1") 2>/dev/null; then
			return 0
		fi

		sleep 1
	done

	return 1
}

ports_listening() {
	ss -Htln | awk -v p="$1" '{ n = split($4, a, ":"); if (a[n] == p) found = 1 } END { exit !found }'
}

# fail DIR MESSAGE shows the end of the iRODS server's log, then dies with
# MESSAGE.
fail() {
	local dir=$1 log=$1/server/var/lib/irods/log/test_mode_output.log

	if [ -f "$log" ]; then
		tail -n 50 "$log" >&2
	elif [ "${runtime:-singularity}" != singularity ]; then
		"$runtime" logs "ibackup-testzone-$(id -un)-$base" 2>&1 | tail -n 50 >&2 || true
	fi

	die "$2"
}

# is_holder PID succeeds if PID is a port lock holder.
is_holder() {
	[ "$(tr '\0' ' ' <"/proc/$1/cmdline" 2>/dev/null)" = "sleep infinity " ]
}

down() {
	local dir=$1 runtime='' name='' singularity_dir='' wr_bin='' wr_port='' wr_web='' holder

	if [ -f "$dir/state" ]; then
		# shellcheck disable=SC1091
		source "$dir/state"
		PATH=$singularity_dir:$PATH

		# Only the zone's own manager can be on its locked ports and dir. It is
		# stopped even if its start reported failure, as it may still be up.
		if [ -d "$dir/wrhome" ]; then
			zone_wr "$dir" manager stop >/dev/null 2>&1 || true
		fi

		if [ "$runtime" = singularity ]; then
			singularity --silent instance stop "$name" >/dev/null 2>&1 || true
		else
			"$runtime" rm -f "$name" >/dev/null 2>&1 || true
		fi
	fi

	# The ports are free for other zones once the server and manager are gone.
	if [ -f "$dir/holder" ]; then
		holder=$(cat "$dir/holder")

		if is_holder "$holder"; then
			kill "$holder" 2>/dev/null || true
		fi
	fi

	chmod -R u+w "$dir" 2>/dev/null || true
	rm -rf "$dir"
}

# tear_down_on_exit DIR has DIR's zone torn down when this script exits.
tear_down_on_exit() {
	# shellcheck disable=SC2064
	trap "trap '' INT TERM HUP; down $(printf %q "$1")" EXIT
	trap 'exit 130' INT
	trap 'exit 143' TERM
	trap 'exit 129' HUP
}

# run_child CMD... runs CMD in a session of its own and returns its exit code.
# On a signal to this script, all of CMD's processes get SIGTERM, and are waited
# for before the zone is torn down. (CMD starts with SIGINT ignored, as for any
# background command of a script, so passing SIGINT on would do nothing.)
run_child() {
	local child rc=0

	setsid "$@" <&0 &
	child=$!

	# shellcheck disable=SC2064
	trap "kill -TERM -- -$child 2>/dev/null; wait $child || true; exit 130" INT
	# shellcheck disable=SC2064
	trap "kill -TERM -- -$child 2>/dev/null; wait $child || true; exit 143" TERM
	# shellcheck disable=SC2064
	trap "kill -TERM -- -$child 2>/dev/null; wait $child || true; exit 129" HUP

	wait "$child" || rc=$?

	return "$rc"
}

run() {
	local dir

	unset_settings
	dir=$(mktemp -d "${TMPDIR:-/tmp}/ibackup-testzone.XXXXXX")
	tear_down_on_exit "$dir"

	SECONDS=0
	up "$dir"
	echo "testzone: ready in ${SECONDS}s" >&2

	# shellcheck disable=SC1091
	source "$dir/env.sh"
	run_child "$@"
}

case "${1:-}" in
run)
	shift
	[ $# -gt 0 ] || die "usage: $0 run CMD..."
	run "$@"
	;;
up)
	[ $# -eq 2 ] || die "usage: $0 up DIR"
	up "$2"
	trap - EXIT INT TERM HUP
	echo "testzone: source $(cd "$2" && pwd)/env.sh from bash to use it" >&2
	;;
down)
	[ $# -eq 2 ] || die "usage: $0 down DIR"
	[ -f "$2/holder" ] || die "$2 has no zone"
	down "$(cd "$2" && pwd)"
	;;
*)
	die "usage: $0 run CMD... | up DIR | down DIR"
	;;
esac
