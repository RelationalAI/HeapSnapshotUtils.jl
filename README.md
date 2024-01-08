# HeapSnapshotUtils.jl

A package with utilities for working with snapshots of Julia's heap. Currently, it only provides a single function: `subsample_snapshot`, which allows you to subsample a snapshot, which is sometimes needed for the snapshot to load in the first place. A short example:

```julia
using HeapSnapshotUtils, Profile
mktemp() do path, io
    Profile.take_heap_snapshot(io)
    flush(io)
    close(io)
    # Subsample the snapshot by removing nodes representing objects and strings which smaller 
    # than 64 bytes and don't contain the string "Tuple" in their name (not relevant for strings).
    # By default, the subsample snapshot is written to the same directory as the original snapshot, with its
    # name prefixed by "subsampled_".
    subsample_snapshot(path) do node_type, node_self_size, node_name
    # node types are 0 based indices into: 
    # ["synthetic", "jl_task_t", "jl_module_t", "jl_array_t", "object","String","jl_datatype_t", "jl_svec_t", "jl_sym_t"]
        node_type in (0,1,2,3,6,7,8) || (node_self_size >= 64) || occursin(r"Tuple"i, node_name)
    end
end
# [ Info: Reading snapshot from "/var/folders/jq/n4hkdx3968z1qw60dlgzgmnr0000gn/T/jl_lE8ntj"
# ┌ Info: BEFORE:  Snapshot contains 1733005 nodes, 5304569 edges, and 371024 strings.
# │ Total size of nodes: 218.551 MiB
# │     synthetic: 73 bytes (74)
# │     jl_task_t: 7.125 KiB (19)
# │   jl_module_t: 54.344 KiB (148)
# │    jl_array_t: 7.841 MiB (171300)
# │        object: 58.453 MiB (715970)
# │        String: 129.973 MiB (418926)
# │ jl_datatype_t: 11.333 MiB (185679)
# │     jl_svec_t: 675.999 KiB (30986)
# └      jl_sym_t: 10.230 MiB (209903)
# ┌ Info: AFTER:   Snapshot contains 1189977 nodes, 3815437 edges, and 257062 strings.
# │ Total size of nodes: 204.194 MiB
# │     synthetic: 73 bytes (74)
# │     jl_task_t: 7.125 KiB (19)
# │   jl_module_t: 54.344 KiB (148)
# │    jl_array_t: 7.841 MiB (171300)
# │        object: 50.706 MiB (403530)
# │        String: 123.363 MiB (188338)
# │ jl_datatype_t: 11.333 MiB (185679)
# │     jl_svec_t: 675.999 KiB (30986)
# └      jl_sym_t: 10.230 MiB (209903)
# [ Info: Writing snapshot to "/var/folders/jq/n4hkdx3968z1qw60dlgzgmnr0000gn/T/subsampled_jl_lE8ntj"
```