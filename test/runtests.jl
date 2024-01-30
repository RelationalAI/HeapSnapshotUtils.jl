using HeapSnapshotUtils
using Profile
using Test

@testset "HeapSnapshotUtils" begin

@testset "No filtering" begin
    mktemp() do path_full, io
        Profile.Profile.take_heap_snapshot(io)
        flush(io)
        close(io)

        path_sample = subsample_snapshot((x...)->true, path_full)
        try

            nodes_full, strings_full, backwards_edges = HeapSnapshotUtils.parse_nodes(path_full)
            nodes_sample, strings_sample, backwards_edges_sample  = HeapSnapshotUtils.parse_nodes(path_sample)
            orphan_nodes = check_orphan_nodes_from_backward_edges(backwards_edges)
            # the following tests only work if there are no orphan nodes in the input snapshot
            if isempty(orphan_nodes)
                @test length(nodes_full.type) == length(nodes_sample.type)
                @test length(nodes_full.name_index) == length(nodes_sample.name_index)
                @test length(nodes_full.id) == length(nodes_sample.id)
                @test length(nodes_full.self_size) == length(nodes_sample.self_size)
                @test length(nodes_full.edge_count) == length(nodes_sample.edge_count)
                @test length(nodes_full.edges.type) == length(nodes_sample.edges.type)
                @test length(nodes_full.edges.name_index) == length(nodes_sample.edges.name_index)
                @test length(nodes_full.edges.to_pos) == length(nodes_sample.edges.to_pos)
                @test length(strings_full) == length(strings_sample)

                # The order of strings is different after we write the snapshot out (even without filtering)
                # So don't compare indices to strings but the strings themselves after lookup
                @test nodes_full.type == nodes_sample.type
                @test strings_full[nodes_full.name_index .+ 1] == strings_sample[nodes_sample.name_index .+ 1]
                @test nodes_full.id == nodes_sample.id
                @test nodes_full.self_size == nodes_sample.self_size
                @test nodes_full.edge_count == nodes_sample.edge_count
                @test nodes_full.edges.type == nodes_sample.edges.type
                # edges.type .== 2 are indices into arrays which don't have a name
                @test strings_full[nodes_full.edges.name_index[nodes_full.edges.type .!= 2] .+ 1] == strings_sample[nodes_sample.edges.name_index[nodes_full.edges.type .!= 2] .+ 1]
                @test nodes_full.edges.to_pos == nodes_sample.edges.to_pos
                @test sort(strings_full) == sort(strings_sample)
            end
        finally
            rm(path_sample, force=true)
        end
    end
end


@testset "50% filtering" begin
    mktemp() do path_full, io
        Profile.Profile.take_heap_snapshot(io)
        flush(io)
        close(io)

        path_sample = subsample_snapshot((x...)->rand() < 0.5, path_full)
        try
            nodes_full, strings_full = HeapSnapshotUtils.parse_nodes(path_full)
            nodes_sample, strings_sample = HeapSnapshotUtils.parse_nodes(path_sample)

            # Test that the sample is roughly 50% of the full snapshot
            @test 0.25length(nodes_full.type) <= length(nodes_sample.type) <= 0.55length(nodes_full.type)
            @test 0.25length(nodes_full.name_index) <= length(nodes_sample.name_index) <= 0.55length(nodes_full.name_index)
            @test 0.25length(nodes_full.id) <= length(nodes_sample.id) <= 0.55length(nodes_full.id)
            @test 0.25length(nodes_full.self_size) <= length(nodes_sample.self_size) <= 0.55length(nodes_full.self_size)
            @test 0.25length(nodes_full.edge_count) <= length(nodes_sample.edge_count) <= 0.55length(nodes_full.edge_count)

            # It is less clear what is the expected ratio for edges and strings
            @test length(nodes_sample.edges.type) <= length(nodes_full.edges.type)
            @test length(nodes_sample.edges.name_index) <= length(nodes_full.edges.name_index)
            @test length(nodes_sample.edges.to_pos) <= length(nodes_full.edges.to_pos)

            @test issubset(strings_sample, strings_full)
            @test issubset(nodes_sample.id, nodes_full.id) # id is the pointer to the node, so they are comparable across snapshot samples

            filtered_out = Set(setdiff(nodes_full.id, nodes_sample.id))
            @test length(filtered_out) > 0
            id_to_pos = Dict(id => pos for (pos, id) in enumerate(nodes_full.id) if !(id in filtered_out))
            cumsum_edges_full = cumsum(nodes_full.edge_count)
            cumsum_edges_sample = cumsum(nodes_sample.edge_count)

            for i in 1:length(nodes_sample)
                id = nodes_sample.id[i]
                edge_count_sample = nodes_sample.edge_count[i]

                edge_count_full = nodes_full.edge_count[id_to_pos[id]]
                # Test that retained nodes kept their original properties (apart from edge count)
                @test edge_count_sample <= edge_count_full
                @test nodes_full.self_size[id_to_pos[id]] == nodes_sample.self_size[i]
                @test nodes_full.type[id_to_pos[id]] == nodes_sample.type[i]
                @test strings_sample[nodes_sample.name_index[i] + 1] == strings_full[nodes_full.name_index[id_to_pos[id]] + 1]

                edge_count_sample == 0 && continue

                # Test thet after sampling, the nodes that we are connected through edges have the same properties
                sampled_edge_idx_range = cumsum_edges_sample[i] - edge_count_sample + 1:cumsum_edges_sample[i]
                sampled_edge_node_ids = nodes_sample.id[nodes_sample.edges.to_pos[sampled_edge_idx_range]]

                full_edge_idx_range = cumsum_edges_full[id_to_pos[id]] - edge_count_full + 1:cumsum_edges_full[id_to_pos[id]]
                # Remove nodes we filtered out from the full snapshot
                filtered_mask = .!in.(nodes_full.id[nodes_full.edges.to_pos[full_edge_idx_range]], Ref(filtered_out))
                full_edge_idxs = full_edge_idx_range[filtered_mask]

                full_edge_node_ids = nodes_full.id[nodes_full.edges.to_pos[full_edge_idxs]]

                # Ids of connected nodes are the same across the original and sampled snapshot
                @test sampled_edge_node_ids == full_edge_node_ids
                # Edge types are the same across the original and sampled snapshot
                @test nodes_full.edges.type[full_edge_idxs] == nodes_sample.edges.type[sampled_edge_idx_range]
                non_elements_samples = nodes_full.edges.type[full_edge_idxs] .!= 2
                non_elements_full = nodes_sample.edges.type[sampled_edge_idx_range] .!= 2
                # Names of connected nodes are the same across the original and sampled snapshot
                @test strings_full[nodes_full.edges.name_index[full_edge_idxs][non_elements_samples] .+ 1] == strings_sample[nodes_sample.edges.name_index[sampled_edge_idx_range][non_elements_full] .+ 1]
            end

        finally
            rm(path_sample, force=true)
        end
    end
end

@testset "Misc filtering" begin
    mktemp() do path_full, io
        Profile.Profile.take_heap_snapshot(io)
        flush(io)
        close(io)


        path_sample = subsample_snapshot(path_full) do node_type, node_size, node_name
            node_type == 4
        end
        try
            nodes_sample, strings_sample = HeapSnapshotUtils.parse_nodes(path_sample)
            # after preserving the parents of the nodes of type 4,
            # we could have nodes of types other than 4
            @test 4 in nodes_sample.type
        finally
            rm(path_sample, force=true)
        end


        path_sample = subsample_snapshot(path_full) do node_type, node_size, node_name
            node_size < 64
        end
        try
            nodes_sample, strings_sample = HeapSnapshotUtils.parse_nodes(path_sample)
            filtered_sizes = filter(x -> x < 64, nodes_sample.self_size)
            @test !isempty(filtered_sizes)
        finally
            rm(path_sample, force=true)
        end


        path_sample = subsample_snapshot(path_full) do node_type, node_size, node_name
            occursin("Array", node_name)
        end
        try
            nodes_sample, strings_sample = HeapSnapshotUtils.parse_nodes(path_sample)
            filtered_names = filter(x -> occursin("Array", x), strings_sample)
            @test !isempty(filtered_names)
        finally
            rm(path_sample, force=true)
        end
    end
end

@testset "check_orphan_nodes_from_backward_edges" begin
    @testset "Empty backward edges" begin
        backward_edges = Vector{Vector{UInt32}}()
        expected = Set{Int32}()
        actual = check_orphan_nodes_from_backward_edges(backward_edges)
        @test actual == expected
    end

    @testset "No orphan nodes" begin
        backward_edges = [[], [1], [1, 2], [3]]
        actual = check_orphan_nodes_from_backward_edges(backward_edges)
        @test isempty(actual)
    end

    @testset "Orphan nodes present" begin
        backward_edges = [[], [1], [1, 2], [], [3], []]
        actual = check_orphan_nodes_from_backward_edges(backward_edges)
        @test !isempty(actual)
        @test length(actual) == 2
        @test 4 in actual
        @test 6 in actual
    end
end

@testset "check orphan nodes" begin
    current_directory = pwd()
    nodes_full, strings_full = HeapSnapshotUtils.parse_nodes("$(current_directory)/test/data/julia-test.heapsnapshot")
    orphan_nodes = check_orphan_nodes(nodes_full)
    @test isempty(orphan_nodes)

    path_sample = subsample_snapshot("$(current_directory)/test/data/julia-test.heapsnapshot") do node_type, node_size, node_name
        node_type in (1,2,3) && occursin(r"Tuple"i, node_name)
    end

    try
        nodes_sample, strings_sample = HeapSnapshotUtils.parse_nodes(path_sample)
        orphan_nodes = check_orphan_nodes(nodes_sample)
        @test isempty(orphan_nodes)
    finally
        rm(path_sample, force=true)
    end
end

@testset "get_backwards_edges" begin
    current_directory = pwd()
    nodes_full, strings_full, backward_edges = HeapSnapshotUtils.parse_nodes("$(current_directory)/test/data/julia-test.heapsnapshot")
    new_backward_edges = get_backwards_edges(nodes_full)
    @test length(backward_edges) == length(new_backward_edges)
    for idx in eachindex(backward_edges)
        @test backward_edges[idx] == new_backward_edges[idx]
    end
end

@testset "subsample the snapshot" begin
    current_directory = pwd()
    path_sample = subsample_snapshot("$(current_directory)/test/data/julia-test.heapsnapshot") do node_type, node_size, node_name
        node_type in (1,2,3) || occursin(r"Tuple"i, node_name)
    end

    try
        nodes_sample, strings_sample = HeapSnapshotUtils.parse_nodes(path_sample)
        @test 3 in nodes_sample.type
        @test 2 in nodes_sample.type
        @test 1 in nodes_sample.type
        tuple_names = filter(x -> occursin(r"Tuple"i, x), strings_sample)
        @test !isempty(tuple_names)
    finally
        rm(path_sample, force=true)
    end
end

end # @testset "HeapSnapshotUtils"
