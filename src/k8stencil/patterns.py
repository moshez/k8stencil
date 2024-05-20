def args_to_list(args, prefix=""):
    for key, value in args.items():
        key = prefix + key.replace("_", "-")
        if isinstance(value, str):
            yield f"--{key}={value}"
        elif isinstance(value, list):
            for subv in value:
                yield f"--{key}={subv}"
        elif isinstance(value, dict):
            new_prefix = f"{prefix}.{key}." if prefix else f"{key}."
            yield from args_to_list(value, new_prefix)

def env_fields_to_env(env_fields):
    for key, value in env_fields.items():
        yield dict(
            name=key.upper(),
            valueFrom=dict(
                fieldRef=dict(
                    fieldPath=value,
                ),
            ),
        )

def address(ports, name):
    return f"0.0.0.0:{ports[name]}"

def ports_to_container_ports(ports):
    for key, value in ports.items():
        yield dict(
            containerPort=value,
            name=key,
        )

def http_probe(ports, failure, period, slug):
    return dict(
        failureThreshold=failure,
        periodSeconds=period,
        httpGet=dict(
            path=f"/-/{slug}",
            port=ports["http"],
            scheme="HTTP",
        )
    )

def get_volume_mounts(mounts):
    for key, value in mounts.items():
        yield dict(
            mountPath=key,
            **value,
        )

def _get_pod_affinity_terms(pod_affinity_labels, namespace):
    for topology_key in ["kubernetes.io/hostname", "topology.kubernetes.io/zone"]:
        yield dict(
            podAffinityTerm=dict(
                labelSelector=dict(
                    matchExpressions=[
                        dict(
                            key=key,
                            operator="In",
                            values=[value],
                        )
                        for key, value in pod_affinity_labels.items()
                    ],
                ),
                namespaces=[namespace],
                topologyKey=topology_key,
            ),
            weight=100,
        )

def get_pod_anti_affinity(pod_affinity_labels, namespace):
    return dict(
        podAntiAffinity=dict(
            preferredDuringSchedulingIgnoredDuringExecution=list(
                _get_pod_affinity_terms(pod_affinity_labels, namespace)
            ),
        ),
    )

