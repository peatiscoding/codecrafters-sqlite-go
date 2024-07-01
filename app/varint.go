package main

import "bytes"

func ReadVarint(reader *bytes.Reader) (int64, int, error) {
	var result int64
	var shift uint
	var bytesRead int

	for {
		// Read a single byte
		b, err := reader.ReadByte()
		if err != nil {
			return 0, bytesRead, err
		}

		bytesRead++

		// Combine the lower 7 bits into the result
		result = result << shift
		result |= int64(b & 0x7F)

		// Check if the MSB is set; if not, we are done
		if b&0x80 == 0 {
			break
		}

		// Shift the result for the next 7 bits
		shift += 7
	}

	return result, bytesRead, nil
}
