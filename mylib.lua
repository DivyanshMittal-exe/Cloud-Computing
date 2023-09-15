#!lua name=mylib

local function add_wc(keys, args)
    
    return 0
end

redis.register_function('add_wc', add_wc)
