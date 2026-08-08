-- Принимает ключ ([KEYS[1]]) и лимит (ARGV[1]).
-- Возвращает true, если удалось добавить соединение
-- Возвращает false, если не удалось добавить соединение(лимит исчерпан).

-- Переключение протокола на RESP3 (чтобы возвращал True/False, а не 1/0)
redis.setresp(3)

local active_connections = tonumber(redis.call('GET', KEYS[1])) or 0
if active_connections >= tonumber(ARGV[1]) then
    return false   -- лимит превышен
end
redis.call('INCR', KEYS[1])
return true
