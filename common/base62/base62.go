package base62

const chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func Encode(id uint64) string {
	if id == 0 {
		return string(chars[0])
	}
	var buf [11]byte
	i := len(buf)
	for id > 0 {
		i--
		buf[i] = chars[id%62]
		id /= 62
	}
	return string(buf[i:])
}

func Decode(code string) uint64 {
	var id uint64
	for _, c := range code {
		id = id*62 + uint64(charIndex(byte(c)))
	}
	return id
}

func charIndex(c byte) uint64 {
	if c >= '0' && c <= '9' {
		return uint64(c - '0')
	}
	if c >= 'A' && c <= 'Z' {
		return uint64(c - 'A' + 10)
	}
	if c >= 'a' && c <= 'z' {
		return uint64(c - 'a' + 36)
	}
	return 0
}
