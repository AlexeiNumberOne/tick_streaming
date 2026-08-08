-- KEYS[1]   - attempts:<source>
-- ARGV[1]   - лимит попыток установить соединение
-- ARGV[2]   - TTL ключа с попытками установить соединение в секундах
-- Возвращает true, если есть место ещё, или время ожидания(pause), если лимит исчерпан.

-- Переключение протокола на RESP3 (чтобы возвращал True/False, а не 1/0)
redis.setresp(3)

local new_attempt = redis.call('INCR', KEYS[1])
-- TTL при создании ключа
if new_attempt == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
end

if new_attempt <= tonumber(ARGV[1]) then
    return true   -- попытка установить соединения разрешена
else
    local pause = redis.call('TTL', KEYS[1])
    return pause   -- лимит превышен, пауза
end
