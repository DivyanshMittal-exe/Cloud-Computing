#!lua name=mylib

local function push_wc(keys, args)

    -- redis.setresp(3)

    local in_stream_key, zsetkey = unpack(keys)

    local consumer_group, id, localCountJSON = unpack(args)

    local can_write = redis.call('XACK', in_stream_key, consumer_group, id)
    -- rds.rds.xack(IN, WcWorker.GROUP, id)

    if can_write == 1 then
        
        local localCount = cjson.decode(localCountJSON)
        for word, count in pairs(localCount) do
            redis.call("ZINCRBY", zsetkey, count, word)
            -- rds.rds.zincrby(COUNT,count,word)
        end
        
    end

    return can_write
end


redis.register_function('push_wc', push_wc)