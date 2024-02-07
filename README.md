# HeapSnapshotUtils.jl

A package with utilities for working with snapshots of Julia's heap. Currently, it only provides a single function: `subsample_snapshot`, which allows you to subsample a snapshot, which is sometimes needed for the snapshot to load in the first place. A short example:

```julia
using HeapSnapshotUtils, Profile
mktemp() do path, io
    Profile.take_heap_snapshot(io)
    flush(io)
    close(io)
    # Subsample the snapshot by removing nodes representing objects and strings which are smaller 
    # than 64 bytes.
    # By default, the subsample snapshot is written to the same directory as the original snapshot, with its
    # name prefixed by "subsampled_".
    subsample_snapshot(path) do node_type, node_self_size, node_name
    # node types are 0 based indices into: 
    # ["synthetic", "jl_task_t", "jl_module_t", "jl_array_t", "object","String","jl_datatype_t", "jl_svec_t", "jl_sym_t"]
        node_type in (0,1,2,3,6,7,8) || (node_self_size >= 64)
    end
end
# [ Info: Reading snapshot from "/var/folders/jq/n4hkdx3968z1qw60dlgzgmnr0000gn/T/jl_R4PJNc"
# ┌ Info: BEFORE:  Snapshot contains 1699809 nodes, 5225819 edges, and 372462 strings.
# │ Total size of nodes: 218.828 MiB
# │     synthetic: 54 bytes (55)
# │     jl_task_t: 3.750 KiB (10)
# │   jl_module_t: 54.344 KiB (148)
# │    jl_array_t: 7.364 MiB (160875)
# │        object: 58.904 MiB (693604)
# │        String: 130.472 MiB (422354)
# │ jl_datatype_t: 11.229 MiB (183973)
# │     jl_svec_t: 674.909 KiB (30925)
# └      jl_sym_t: 10.142 MiB (207865)
# ┌ Info: AFTER:   Snapshot contains 897672 nodes, 3173711 edges, and 187713 strings.
# │ Total size of nodes: 166.966 MiB
# │     synthetic: 54 bytes (55)
# │     jl_task_t: 2.625 KiB (7)
# │   jl_module_t: 54.344 KiB (148)
# │    jl_array_t: 5.400 MiB (117971)
# │        object: 41.002 MiB (320353)
# │        String: 102.871 MiB (133186)
# │ jl_datatype_t: 8.579 MiB (140558)
# │     jl_svec_t: 602.469 KiB (27606)
# └      jl_sym_t: 8.470 MiB (157788)
# [ Info: Writing snapshot to "/var/folders/jq/n4hkdx3968z1qw60dlgzgmnr0000gn/T/subsampled_jl_R4PJNc"
```