using REPL.TerminalMenus

function get_retained_size!(retained_size, idoms::Vector, scratch)
    n = length(idoms)
    level1 = UInt32[]

    ndominated = scratch
    fill!(ndominated, 0)
    for i in 1:n
        idom = idoms[i]
        idom == 0 && continue
        ndominated[idom] += 1
    end
    worklist = [UInt32(i) for i in 1:n if ndominated[i] == 0]

    while !isempty(worklist)
        i = pop!(worklist)
        idom = idoms[i]
        idom == 0 && continue
        retained_size[idom] += retained_size[i]
        idom == 1 && push!(level1, i)
        ndominated[idom] -= 1
        if ndominated[idom] == 0
            push!(worklist, idom)
        end
    end
    sort!(level1, by=x->retained_size[x], rev=true)

    return level1
end

struct SnapshotTUIData
    nodes::Nodes
    domtree::Vector{UInt32}
    retained_size::Vector{Int}
    level1::Vector{UInt32}
end

function SnapshotTUIData(nodes::Nodes)
    fill_back_edges_and_cumcounts!(nodes)
    scratch = Vector{UInt32}(undef, length(nodes))

    @time "get_domtree" domtree = get_domtree(nodes, scratch)
    retained_size = copy(nodes.self_size)
    @time "get_retained_size!" level1 = get_retained_size!(retained_size, domtree, scratch)

    return SnapshotTUIData(nodes, domtree, retained_size, level1)
end
Base.length(n::SnapshotTUIData) = length(n.nodes)
Base.@propagate_inbounds Base.getindex(n::SnapshotTUIData, i) = n.nodes[i]


# Adapted from timing.jl
const _mem_pow10_units = ["B", "KB", "MB", "GB", "TB", "PB"]
function _format_pow10_bytes(bytes)
    bytes, mb = Base.prettyprint_getunits(bytes, length(_mem_pow10_units), Int64(1000))
    if mb == 1
        return string(Int(bytes), " ", _mem_pow10_units[mb])
    else
        return string(Base.Ryu.writefixed(Float64(bytes), 3), " ", _mem_pow10_units[mb])
    end
end

function show_nodes(nnodes, strings, root_node::AbstractString, max_depth)
    for i in 1:length(nnodes)
        root_node == strings[nnodes.nodes.name_index[i]+1] || continue
        show_nodes(nnodes, strings, i, max_depth)
    end
end

function show_nodes(nnodes, strings, root_node::Integer, max_depth)
    1 <= root_node <= length(nnodes) || throw(ArgumentError("`root_node` must be between 1 and $(length(n)), got $root_node"))
    nodes = nnodes.nodes
    edges = nodes.edges
    if max_depth < 0
        _show_nodes(nnodes, strings, UInt32(root_node), abs(Int(max_depth)) + 1, 0, 0, edges._from_pos, nodes._back_cumcount, false)
    else
        _show_nodes(nnodes, strings, UInt32(root_node), Int(max_depth) + 1, 0, 0, edges.to_pos, nodes._edge_cumcount, true)
    end
end


function _show_nodes(nnodes, strings, node, maxdepth, _depth, _edge, _pos, _cs, up)
    _depth == maxdepth && return
    nodes = nnodes.nodes
    prev_cnt = get(_cs, node - 1, 0)
    cnt = _cs[node]
    children = @view _pos[prev_cnt+1:cnt]
    if _depth != 0
        up ? print("    " ^ _depth, "-->") : print("    " ^ _depth, "<")
        edge_index = nodes.edges.name_index[_edge]+1
        edge_label = get(strings, edge_index, "")
        printstyled(edge_label, color=:blue)
        print("/")
        printstyled(EDGE_TYPES[nodes.edges.type[_edge]+1], color=:magenta)
        up ? print("> ") : print("<-- ")
    end
    print("\"")
    printstyled(strings[nodes.name_index[node]+1], bold=true, color=:blue)
    print("\"/")
    printstyled(NODE_TYPES[nodes.type[node]+1], color=:magenta)
    print(" @ ")
    printstyled(string(node), color=:light_cyan)
    print(" (↓")
    printstyled(nodes.edge_count[node], color=:green)
    print(", ↑"),
    printstyled(nodes._back_count[node], color=:red)
    print(") | ")
    printstyled(_format_pow10_bytes(nodes.self_size[node]), color=:yellow)
    print(" / ")
    printstyled(_format_pow10_bytes(nnodes.retained_size[node]), color=:yellow, bold=true)
    println()
    for (e, child) in zip(prev_cnt+1:cnt, children)
        _show_nodes(nnodes, strings, child, maxdepth, _depth+1, e, _pos, _cs, up)
    end
end
