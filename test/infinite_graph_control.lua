-- support uri parameters like: "control?target=read_write_duration&step=10m&op=more"
-- target: read_write_duration
-- step: 10m / 1h / 12h (means 10 minutes / 1 hours / 12 hours)
-- op: more / less  (means increace / decreace)

local conf_file = '/usr/local/sandai/redis-c-cluster/infinite/infinite.conf'
local notify_file = '/usr/local/sandai/redis-c-cluster/infinite/infinite.notify'

local config_args = {}

config_args["read_write_duration"] = 0
config_args["cost_duration"] = 0
config_args["ttl_duration"] = 0

function load_config()
    local fp,err = io.open(conf_file, 'r')
    if not fp then
        ngx.log(ngx.INFO, "fail to open config file for read: ".. err)
        return false
    end

    for line in fp:lines() do
        local key,val = line:match("%s*(%S+)%s+(%S+)")
        --ngx.log(ngx.DEBUG, "load_config read:["..key.. "]->["..val.."]")
        if key == "read_write_duration" or key == "cost_duration" or key == "ttl_duration" then
            if not val then
                ngx.say('current value of configuration ['.. key .. '] not exist')
                return false
            end
            config_args[key] = tonumber(val)
        end
    end

    fp:close()
    return true
end

function write_config()
    local fp,err = io.open(conf_file, 'w')
    if not fp then
        ngx.say('error in open config file for write: '.. err)
        return false
    end

    for key,val in pairs(config_args) do
        local ret,err = fp:write(key.." "..val.."\n")
        if not ret then
            ngx.say('error in write config file '..conf_file..' : '.. err)
            return false
        end
    end

    fp:close()
    return true
end

function parse_step(unit_str)
    --ngx.log(ngx.DEBUG, "pare_step:"..unit_str)
    if unit_str == "10m" then
        return 60*10
    elseif unit_str == "1h" then
        return 60 * 60
    elseif unit_str == "12h" then
        return 60 * 60 * 12
    else
        return 0
    end
end

if not load_config() then
    ngx.log(ngx.INFO, "config file ".. conf_file .." will be new created")
end

local args = ngx.req.get_uri_args()
local step = parse_step(args["step"])

if step <= 0 then
    ngx.say('invalid config step ' .. args["step"])
    return
end

local target = args["target"]
--ngx.log(ngx.DEBUG, "target:"..args["target"])

if target and config_args[target] then
    if args["op"]  == "more" then
        config_args[target] = config_args[target] + step
    elseif args["op"]  == "less" then
        config_args[target] = config_args[target] - step
    else
        ngx.say('invalid config step ' .. args["step"])
        return
    end

    config_args[target] = config_args[target] - (config_args[target] % 10)
    if(config_args[target] <= 0) then
        config_args[target] = 1
    end
end

if target and args["op"] then
    ngx.log(ngx.DEBUG, "change " .. target .. " to ".. args["op"] .. " " .. step)
else
    local req_para = ""
    for k,v in pairs(args) do
       req_para = req_para .. k .. ":" .. v.. " ,"
    end
    ngx.log(ngx.DEBUG, "invalid command parameter:" .. req_para)
end

if not write_config() then
    return
end

os.execute('touch '..notify_file)
ngx.sleep(1)
ngx.redirect("/")
