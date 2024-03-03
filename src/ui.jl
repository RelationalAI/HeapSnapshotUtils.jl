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
    strings::Vector{String}
    domtree::Vector{UInt32}
    retained_size::Vector{Int}
    level1::Vector{UInt32}
end

function SnapshotTUIData(nodes::Nodes, strings)
    Base.isinteractive() && _progress_print("Calculating backedges")
    fill_back_edges_and_cumcounts!(nodes)
    scratch = Vector{UInt32}(undef, length(nodes))

    domtree = get_domtree(nodes, scratch)
    retained_size = copy(nodes.self_size)
    Base.isinteractive() && _progress_print("Calculating retained size")
    level1 = get_retained_size!(retained_size, domtree, scratch)
    Base.isinteractive() && _progress_print()
    return SnapshotTUIData(nodes, strings, domtree, retained_size, level1)
end
Base.length(n::SnapshotTUIData) = length(n.nodes)
Base.@propagate_inbounds Base.getindex(n::SnapshotTUIData, i) = n.nodes[i]

# Adapted from timing.jl
const _mem_pow10_units = ["B", "KB", "MB", "GB", "TB", "PB"]
function _print_size(io, bytes, accent, bold)
    bytes, mb = Base.prettyprint_getunits(bytes, length(_mem_pow10_units), Int64(1000))
    bytes_str = mb == 1 ? string(Int(bytes)) : Base.Ryu.writefixed(Float64(bytes), 3)
    printstyled(io, bytes_str, color=:yellow, underline=accent, bold=bold)
    printstyled(io, _mem_pow10_units[mb], color=bold ? :yellow : :default, underline=accent)
end
_short(str) = (str = replace(str, '\n' => "\\n"); ncodeunits(str) > 42 ? string(first(str, 41), "…") : str)
_indent(io, n) = print(io, "    " ^ n)
function _show_edge(io, nnodes, edge, accent, direction, show_edges)
    nodes = nnodes.nodes
    printstyled(io, direction ? "-→ " : "←- ", underline=accent)
    show_edges || return
    printstyled(io, "\"", underline=accent)
    edge_index = nodes.edges.name_index[edge]+1
    egde_type_idx = nodes.edges.type[edge]+1
    edge_label = egde_type_idx == 3 ? string(edge_index-1) : # "element" type
        egde_type_idx == 2 ? "<native>" : # "hidden" type
        _short(get(nnodes.strings, edge_index, ""))
    printstyled(io, edge_label, color=:blue, underline=accent)
    printstyled(io, "\"", underline=accent)
    printstyled(io, "/", underline=accent)
    printstyled(io, EDGE_TYPES2[egde_type_idx], color=:magenta, underline=accent)
    printstyled(io, direction ? " → " : " ← ", underline=accent)
end
function _show_node(io, nnodes, node, accent, direction, show_idxs, show_self_size, show_both_degrees)
    nodes = nnodes.nodes
    printstyled(io, "\"", underline=accent)
    printstyled(io, _short(nnodes.strings[nodes.name_index[node]+1]), bold=true, color=:blue, underline=accent)
    printstyled(io, "\"/", underline=accent)
    printstyled(io, NODE_TYPES2[nodes.type[node]+1], color=:magenta, underline=accent)
    if show_idxs
        printstyled(io, " @ ", underline=accent)
        printstyled(io, string(node), color=:light_cyan, underline=accent)
    end
    if direction
        printstyled(io, " (↓", underline=accent)
        printstyled(io, nodes.edge_count[node], color=:green, underline=accent)
        if show_both_degrees
            printstyled(io, ", ↑", underline=accent),
            printstyled(io, nodes._back_count[node], color=:red, underline=accent)
        end
    else
        printstyled(io, " (↑", underline=accent),
        printstyled(io, nodes._back_count[node], color=:red, underline=accent)
        if show_both_degrees
            printstyled(io, ", ↓", underline=accent)
            printstyled(io, nodes.edge_count[node], color=:green, underline=accent)
        end
    end
    printstyled(io, ") ", underline=accent)
    if show_self_size
        _print_size(io, nodes.self_size[node], accent, false)
        printstyled(io, " / ", underline=accent)
    end
    _print_size(io, nnodes.retained_size[node], accent, true)
end

mutable struct HeapSnapshotMenu <: TerminalMenus.AbstractMenu
    snapshot::SnapshotTUIData

    pagesize::Int
    pageoffset::Int
    cursor::Int
    direction::Bool

    show_idxs::Bool
    show_edges::Bool
    show_self_size::Bool
    show_both_degrees::Bool

    should_continue::Bool
    listen::Bool

    nodes::Vector{UInt32}
    edges::Vector{UInt32}
    depths::Vector{UInt8}
end
function HeapSnapshotMenu(snapshot::SnapshotTUIData)
    nodes = UInt32[1]
    edges = UInt32[0]
    depths = UInt8[0]
    pagesize = 1
    pageoffset = 0
    cursor = 1
    listen = false
    return HeapSnapshotMenu(snapshot, pagesize, pageoffset, cursor, true, true, true, true, true, true, listen, nodes, edges, depths)
end
function TerminalMenus.header(m::HeapSnapshotMenu)
    """
    [r]eset, [p]arent [f]ocus, [l]arge, [m]atch, [i]ds, [s]elf-size, [e]dges, [d]egrees, [v]erbose, [q]uit
    Selected: $(length(m.nodes)) node$(length(m.nodes) == 1 ? "" : "s")
    """
end

function _num_unfolded(m::HeapSnapshotMenu, cursor::Int)
    node = m.nodes[cursor]
    depth = m.depths[cursor]
    edge_count = m.direction ? m.snapshot.nodes.edge_count : m.snapshot.nodes._back_count
    # We assume that if a node is unfolded, all of its children are in the list
    # If we want to poke holes in the list, we need to keep track of the number of children
    # by iterating through the list and counting the number of nodes with depth + 1 of the parent
    cnt = edge_count[node]
    unfolded = false
    if cnt > 0 && length(m.nodes) > cursor
        unfolded = m.depths[cursor+1] == (depth + UInt8(1))
    end
    return unfolded * cnt
end

function TerminalMenus.pick(m::HeapSnapshotMenu, cursor::Int)
    _num_unfolded(m, cursor) > 0 ? fold!(m, cursor) : unfold!(m, cursor)
    m.should_continue = true
    return true
end

function fold!(m::HeapSnapshotMenu, cursor::Int)
    node = m.nodes[cursor]
    edge_count = m.direction ? m.snapshot.nodes.edge_count : m.snapshot.nodes._back_count

    n_items_to_delete = edge_count[node]
    if n_items_to_delete > 0
        new_cursor = cursor + 1
        while n_items_to_delete > 0
            n_items_to_delete += _num_unfolded(m, new_cursor)
            n_items_to_delete -= UInt32(1)
            new_cursor += 1
        end
        splice!(m.nodes, cursor+1:new_cursor-1)
        splice!(m.edges, cursor+1:new_cursor-1)
        splice!(m.depths, cursor+1:new_cursor-1)
    end
end

function unfold!(m::HeapSnapshotMenu, cursor::Int)
    node = m.nodes[cursor]
    depth = m.depths[cursor]
    edge_count = m.direction ? m.snapshot.nodes.edge_count : m.snapshot.nodes._back_count

    if edge_count[node] > 0
        new_cursor = cursor
        neighbors = m.direction ? _out_edges_and_nodes(m.snapshot.nodes, node) : _in_edges_and_nodes(m.snapshot.nodes, node)
        for (e, u) in neighbors
            new_cursor += 1
            insert!(m.nodes, new_cursor, u)
            insert!(m.edges, new_cursor, e)
            insert!(m.depths, new_cursor, depth+true)
        end
    end
end

function reset_tree!(m::HeapSnapshotMenu, node::UInt32=UInt32(1), direction::Bool=true)
    empty!(m.nodes)
    empty!(m.edges)
    empty!(m.depths)
    if node > 0
        push!(m.nodes, node)
        push!(m.edges, 0)
        push!(m.depths, 0)
    end
    m.direction = direction
    m.cursor = 1
    return true
end

function focus!(m::HeapSnapshotMenu, cursor::Int)
    reset_tree!(m, m.nodes[cursor])
    unfold!(m, 1)
    return true
end

function reverse_tree!(m::HeapSnapshotMenu, cursor::Int)
    reset_tree!(m, m.nodes[cursor], !m.direction)
    unfold!(m, 1)
    return true
end

function largest_retainers!(m::HeapSnapshotMenu)
    reset_tree!(m, UInt32(0))
    for i in 1:min(length(m.snapshot.level1), max(100, m.pagesize))
        push!(m.nodes, m.snapshot.level1[i])
        push!(m.edges, 0)
        push!(m.depths, 0)
    end
    return true
end

const DEFAULT_NODE_TYPE_BITSET = BitSet([0,1,2,3,4,6,7,8]) # i.e. no Strings
function query!(m::HeapSnapshotMenu, pattern::Union{Regex,AbstractString}, minsize=0, node_type::BitSet = DEFAULT_NODE_TYPE_BITSET)
    reset_tree!(m, UInt32(0))
    # TODO: multithread?
    for i in UInt32(1):UInt32(length(m.snapshot.nodes))
        str = m.snapshot.strings[m.snapshot.nodes.name_index[i]+1]
        retained = m.snapshot.retained_size[i]
        typ = m.snapshot.nodes.type[i]
        if occursin(pattern, str) && retained >= minsize && typ in node_type
            push!(m.nodes, i)
            push!(m.edges, 0)
            push!(m.depths, 0)
        end
    end
    !isempty(m.nodes) && sort!(m.nodes, by=x->m.snapshot.retained_size[x], rev=true)
    return true
end

function TerminalMenus.keypress(m::HeapSnapshotMenu, i::UInt32)
    c = Char(i)
    if c == 'f'
        m.should_continue = true
        return focus!(m, m.cursor)
    elseif c == 'p'
        m.should_continue = true
        return reverse_tree!(m, m.cursor)
    elseif c == 'r'
        m.should_continue = true
        return reset_tree!(m)
    elseif c == 'l'
        m.should_continue = true
        return largest_retainers!(m)
    elseif c == 'i'
        m.show_idxs = !m.show_idxs
        return false
    elseif c == 's'
        m.show_self_size = !m.show_self_size
        return false
    elseif c == 'd'
        m.show_both_degrees = !m.show_both_degrees
        return false
    elseif c == 'e'
        m.show_edges = !m.show_edges
        return false
    elseif c == 'm'
        m.listen = true
        m.should_continue = true
        return true
    elseif c == 'v'
        if m.show_idxs && m.show_self_size && m.show_edges && m.show_both_degrees
            m.show_idxs = false
            m.show_self_size = false
            m.show_edges = false
            m.show_both_degrees = false
        else
            m.show_idxs = true
            m.show_self_size = true
            m.show_edges = true
            m.show_both_degrees = true
        end
        return false
    end

    return false
end
TerminalMenus.numoptions(m::HeapSnapshotMenu) = length(m.nodes)
TerminalMenus.cancel(m::HeapSnapshotMenu) = m.should_continue = false
TerminalMenus.selected(m::HeapSnapshotMenu) = m.nodes[m.cursor]

function TerminalMenus.writeline(buf::IO, m::HeapSnapshotMenu, idx::Int, iscursor::Bool)
    depth = m.depths[idx]
    node = m.nodes[idx]
    iscursor && (m.cursor = idx)
    colored_buf = IOContext(buf, :color => true)
    if depth > 0
        edge = m.edges[idx]
        _indent(colored_buf, depth)
        _show_edge(colored_buf, m.snapshot, edge, iscursor, m.direction, m.show_edges)
    end
    _show_node(colored_buf, m.snapshot, node, iscursor, m.direction, m.show_idxs, m.show_self_size, m.show_both_degrees)
end

function browse()
    mktemp() do path, io
        Base.isinteractive() && _progress_print("Taking a heap snapshot")
        Profile.Profile.take_heap_snapshot(io)
        Base.isinteractive() && _progress_print()
        close(io)
        browse(path)
    end
end
function browse(file::Union{AbstractString,IO})
    nodes, strings = parse_nodes(file)
    tdata = SnapshotTUIData(nodes, strings)
    browse(tdata)
end
function browse(tdata::SnapshotTUIData)
    m = HeapSnapshotMenu(tdata)
    while m.should_continue
        m.should_continue = false
        m.pagesize = max(1, min(length(m.nodes), displaysize(IOContext(stdout, :limit=>true))[1] - 2))
        if m.listen # Apparently, we need to listen for user input outside of the menu loop
            try
                Base.isinteractive() && _progress_print("regex for node names [\"\"]: ")
                pattern = Regex(readline())
                default_size = pattern == r"" ? 32*1024*1024 : 0
                print("minimal retained size in bytes [$default_size]: ")
                minsize = let s = readline(); isempty(s) ? default_size : parse(Int, strip(s)) end
                print("node types, 1 to $(length(NODE_TYPES)), comma separated [1,2,3,4,5,7,8,9]: ")
                node_types = let s = readline();
                    isempty(s) ? DEFAULT_NODE_TYPE_BITSET :
                        BitSet(filter!(x->1 <= x <= 9, parse.(Int, strip.(split(s, ',')))) .- 1)
                end
                query!(m, pattern, minsize, node_types)
            catch e
                !(e isa InterruptException) && rethrow()
            finally
                m.listen = false
            end
        end
        Base.isinteractive() && _progress_print()
        request(m; cursor=m.cursor)
    end
end
