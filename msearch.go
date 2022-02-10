// Package msearch  基于mmap技术的，以本地文件为基础的搜索技术。提供增加、删、查（简单的替代mysql。）
// 单个 value 长度不能超过255. // todo if needed?
// [_8(total) _1 key  _1(len) xxx _1(len) xxx  _8(next) _8(overflow offset)]

package msearch

import (
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"sync"
	"syscall"
)

// notExist 标记不存在的key. // TODO 好像这个标记没什么用
const notExist = -1

// DefaultLength 默认映射空间大小 64 GB，不影响实际内存大小。
const DefaultLength = 64 << 30

type MSearcher interface {
	Add(key string, values ...string) error
	Del(key string, values ...string)
	Get(key string) []string
	DelByPrefix(key string, values ...string)
	Update(key string, values ...string) error
	Exist(key string) bool
}

// Msearch  It's safe for concurrent use by multiple goroutines.
type Msearch struct {
	mu sync.RWMutex // mu to protect the follow fields
	f  *os.File     // After the syscall.Mmap() call has returned, the file descriptor, fd, can be closed immediately
	// without invalidating the mapping. But after f.Close(), we can't write any data to the file.
	// So, the f should not call Close().
	offset    int            // last offset of the f
	keyMap    map[string]int // store all keys, value is the offset in bytesAddr of every key
	bytesAddr []byte         // bytesAddr is the virtual address space of the process
}

// NewMsearch create a new Msearch by file and length。
// file is the path of the underlying file.
// the length argument specifies the length of the mapping (which must be greater than 0)
// it has no impact on the real memory. the default value is 64GB.
func NewMsearch(file string, length int) (*Msearch, error) {
	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	if length <= 0 {
		length = DefaultLength
	}
	// 追加用f.Write 读取和修改用MMap
	bytesAddr, err := syscall.Mmap(int(f.Fd()), 0, length, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &Msearch{
		mu:        sync.RWMutex{},
		f:         f,
		offset:    0,
		keyMap:    make(map[string]int, 1<<10),
		bytesAddr: bytesAddr,
	}, nil
}

// Get one or more value.
func (s *Msearch) Get(key string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.gets(key)
}

// Add one or more value.
func (s *Msearch) Add(key string, values ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.adds(key, values...)
}

// Del one or more value.
func (s *Msearch) Del(key string, values ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dels(key, values...)
}

// DelByPrefix 根据前缀删除.
func (s *Msearch) DelByPrefix(key string, values ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delsPrefix(key, values...)
}

// Update 更新。先删除所有老数据，然后更新新数据.
func (s *Msearch) Update(key string, values ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	oldValues := s.gets(key)
	s.dels(key, oldValues...)
	err := s.adds(key, values...)
	return err
}

func (s *Msearch) Exist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if offset, ok := s.keyMap[key]; ok && offset != notExist {
		return true
	}
	s.keyMap[key] = notExist
	return false
}
func (s *Msearch) delsPrefix(key string, values ...string) {
	offset, ok := s.keyMap[key]
	if !ok {
		return
	}

	if len(values) == 0 {
		return
	}
	for {
		d := s.delPrefix(offset, values...)
		if d == 0 {
			break
		}
		offset = d
	}
}

func (s *Msearch) dels(key string, values ...string) {
	offset, ok := s.keyMap[key]
	if !ok {
		return
	}
	valueMap := make(map[string]struct{}, len(values))
	for _, value := range values {
		valueMap[value] = struct{}{}
	}
	if len(valueMap) == 0 {
		return
	}
	for {
		d := s.del(offset, valueMap)
		if d == 0 {
			break
		}
		offset = d
	}
}

func (s *Msearch) gets(key string) []string {
	offset, ok := s.keyMap[key]
	if !ok || offset == notExist {
		return nil
	}
	var lists []string
	var d int
	for {
		var list []string
		list, d = s.get(offset)
		lists = append(lists, list...)
		if d == 0 {
			break
		}
		offset = d
	}
	return lists
}

// empty 插入判断是否有空位，以及空位的长度.
func (s *Msearch) empty(offset int) (o int, start int, end int, t bool) {
	var lastDec int
	for {
		o, lastDec, start, end, t = s.empty1(offset)
		if lastDec == 0 || t {
			break
		}
		offset = lastDec
	}
	return
}

// getB8byOffset 这个offset是每个value的起始offset 得到最后的一个8位 offset只能通过s.keyMap 获得。
func (s *Msearch) getB8byOffset(offset int) (b8 []byte) {
	var lastDec int
	for {
		lastDec, b8 = s.b8(offset)
		if lastDec == 0 {
			break
		}
		offset = lastDec
	}
	return
}

// empty1 是否有空位，以及空位的长度.
func (s *Msearch) empty1(offset int) (o int, lastDec int, start int, end int, t bool) {
	// t为false的时候 也就是没有空位 有b8
	var first bool
	total := bigUint64(s.bytesAddr[offset : offset+8])
	b := s.bytesAddr[offset : offset+total]
	o = offset
	for i := int(b[8] + 1 + 8); i < len(b[:len(b)-16]); {
		if b[i] == 0 {
			if !first {
				first = true
				t = true
				start = i
			}
			i++
			continue
		}
		if t {
			end = i
			return
		}
		i += int(b[i]) + 1
	}
	if t && end == 0 {
		end = total - 16
	}
	lastDec = bigUint64(b[total-8 : total])
	return
}

func (s *Msearch) b8(offset int) (lastDec int, b8 []byte) {
	// t为false的时候 也就是没有空位 有b8
	if offset >= s.offset {
		return 0, nil
	}
	total := bigUint64(s.bytesAddr[offset : offset+8])
	b8 = s.bytesAddr[offset+total-8 : offset+total]
	b := s.bytesAddr[offset : offset+total]
	lastDec = bigUint64(b[total-8 : total])
	return
}

func (s *Msearch) add(b8 []byte, key string, values ...string) (int, error) {
	var b = make([]byte, 1<<10)
	b[8] = byte(len(key))
	n := copy(b[9:], key)
	idx := n + 1 + 8
	for _, value := range values {
		if len(b) < idx+len(value)+2 {
			// 容量不足就扩容 扩容一定要覆盖下面的copy
			b = append(b, make([]byte, 1<<10)...)
		}
		// todo len(value)大于255？？
		if len(value) > 255 {
			return 0, errors.New("value exceed max length 255")
		}
		b[idx] = byte(len(value))
		// 一定要注意copy的地方
		copy(b[idx+1:], value)
		idx += 1 + len(value)
	}
	total := idx + 16
	binary.BigEndian.PutUint64(b[idx:], uint64(total+s.offset)) // todo 是否有必要？？
	b = b[:total]
	binary.BigEndian.PutUint64(b[:8], uint64(total))
	_, err := s.f.Write(b)
	if err != nil {
		return 0, err
	}
	if i, ok := s.keyMap[key]; !ok || i == notExist {
		s.keyMap[key] = s.offset

	}
	if len(b8) > 0 {
		// 末尾的
		binary.BigEndian.PutUint64(b8, uint64(s.offset))
	}
	s.offset += total
	return total, err
}
func (s *Msearch) adds(key string, values ...string) error {
	if len(values) == 0 {
		return nil
	}
	offset, ok := s.keyMap[key]
	// 不存在
	if !ok || offset == notExist {
		_, err := s.add(nil, key, values...)
		return err
	}
	// t 是否能插空 插空进入
	// s.bytesAddr[offset:offset+8]
	if len(values) == 1 {
		value := values[0]
		o, start, end, t := s.empty(offset)
		if t && len(value) < (end-start) {
			total := bigUint64(s.bytesAddr[offset : offset+8])
			b := s.bytesAddr[o : o+total]
			b[start] = byte(len(value))
			copy(b[start+1:], value)
			return nil
		}
	}
	b8 := s.getB8byOffset(offset)
	_, err := s.add(b8, key, values...)
	return err
}

func (s *Msearch) del(offset int, valueMap map[string]struct{}) int {
	total := bigUint64(s.bytesAddr[offset : offset+8])
	if total == 0 {
		return 0
	}
	b := s.bytesAddr[offset : offset+total]
	for i := int(b[8] + 1 + 8); i < len(b[:len(b)-16]); {
		bi := int(b[i])
		if bi == 0 {
			i++
			continue
		}
		value := string(b[i+1 : i+1+int(b[i])])
		if _, ok := valueMap[value]; ok {
			copy(b[i+1:i+1+int(b[i])], make([]byte, int(b[i])))
			b[i] = 0
		}
		i += bi + 1

	}
	return bigUint64(b[total-8 : total])
}
func (s *Msearch) delPrefix(offset int, values ...string) int {
	total := bigUint64(s.bytesAddr[offset : offset+8])
	if total == 0 {
		return 0
	}
	b := s.bytesAddr[offset : offset+total]
	for i := int(b[8] + 1 + 8); i < len(b[:len(b)-16]); {
		bi := int(b[i])
		if bi == 0 {
			i++
			continue
		}
		value := string(b[i+1 : i+1+int(b[i])])
		for _, v := range values {
			if strings.HasPrefix(value, v) {
				copy(b[i+1:i+1+int(b[i])], make([]byte, int(b[i])))
				b[i] = 0
			}
		}
		i += bi + 1

	}
	return bigUint64(b[total-8 : total])
}

func (s *Msearch) get(offset int) ([]string, int) {
	total := bigUint64(s.bytesAddr[offset : offset+8])
	b := s.bytesAddr[offset : offset+total]
	var list []string
	for i := int(b[8] + 1 + 8); i < len(b[:len(b)-16]); {
		if b[i] == 0 {
			i++
			continue
		}
		list = append(list, string(b[i+1:i+1+int(b[i])]))
		i += int(b[i]) + 1
	}
	lastDec := bigUint64(b[total-8 : total])
	return list, lastDec
}

// bigUint64 对大数字进行解码 长度为0-8位的字节切片. binary.BigEndian.PutUint64 是编码.
func bigUint64(buf []byte) int {
	if len(buf) > 8 {
		return 0
	}
	var x int
	for _, b := range buf {
		x = x<<8 | int(b)
	}
	return x
}
