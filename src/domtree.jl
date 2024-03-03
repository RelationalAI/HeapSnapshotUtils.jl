function snca_compress_worklist!(
    labels::Vector{UInt32},
    ancestors::Vector{UInt32},
    v::UInt32,
    last_linked::UInt32,
    worklist::Vector{Tuple{UInt32, UInt32}}
)
    @assert isempty(worklist)
    u = ancestors[v]
    push!(worklist, (u,v))
    @assert u < v
    while !isempty(worklist)
        u, v = last(worklist)
        if u >= last_linked
            if ancestors[u] >= last_linked
                push!(worklist, (ancestors[u], u))
                continue
            end
            if labels[u] < labels[v]
                labels[v] = labels[u]
            end
            ancestors[v] = ancestors[u]
        end
        pop!(worklist)
    end
end

function get_preorder!(to_pre::Vector{UInt32}, to_parent_pre::Vector{UInt32}, from_pre, nodes, worklist::Vector{Tuple{UInt32, UInt32}})
    @assert isempty(worklist)

    t = UInt32(1)
    push!(worklist, (UInt32(1), UInt32(0)))

    while !isempty(worklist)
        v, parent_t = pop!(worklist)
        if iszero(to_pre[v])
            to_pre[v] = t
            to_parent_pre[t] = parent_t
            from_pre[t] = v
            for w in _out_nodes(nodes, v)
                if iszero(to_pre[w])
                    push!(worklist, (w, t))
                end
            end
            t += true
        end
    end

    # If all nodes are reachable, this is length(nodes).
    return t - true
end

function get_domtree(nodes, scratch::Vector{UInt32})
    n = length(nodes)

    worklist = Vector{Tuple{UInt32, UInt32}}()

    to_pre = zeros(UInt32, n) # 0 signals not visited
    from_pre = scratch
    idoms_pre = Vector{UInt32}(undef, n)
    ancestors = Vector{UInt32}(undef, n)

    labels = collect(UInt32(1):UInt32(n))
    semi = fill(typemax(UInt32), n)

    Base.isinteractive() && _progress_print("Calculating preorder index")
    n_reachable = get_preorder!(to_pre, idoms_pre, from_pre, nodes, worklist)

    for w in n_reachable+1:n
        labels[w] = semi[w]
        idoms_pre[w] = UInt32(0)
    end

    Base.isinteractive() && _progress_print("Constructing dominator tree")
    ancestors .= idoms_pre
    for w in reverse(UInt32(2):UInt32(n_reachable))
        semi_w = typemax(UInt32)
        last_linked = w + true
        for v in _in_nodes(nodes, from_pre[w])
            v == 0 && continue
            v_pre = to_pre[v]
            # Ignore unreachable predecessors
            v_pre == 0 && continue

            if v_pre >= last_linked
                empty!(worklist)
                snca_compress_worklist!(labels, ancestors, v_pre, last_linked, worklist)
            end
            semi_w = min(semi_w, labels[v_pre])

        end
        semi[w] = semi_w
        labels[w] = semi_w
    end

    for v in UInt32(2):UInt32(n)
        idom = idoms_pre[v]
        semi_v = semi[v]
        while idom > semi_v
            idom = idoms_pre[idom]
        end
        idoms_pre[v] = idom
    end

    idoms = ancestors # reuse ancestors, since we don't need it anymore
    idoms[1] = 0
    for i in UInt32(2):UInt32(n)
        to_pre[i] == 0 && continue # unreachable
        ip = idoms_pre[to_pre[i]]
        idoms[i] = ip == 0 ? 1 : from_pre[ip]
    end

    return idoms
end
