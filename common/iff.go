package common

// GetBlocksRoundedUp returns the number of blocks given sie, rounded up
func GetBlocksRoundedUp(size uint64, blockSize uint64) uint16 {
	return uint16(size/blockSize) + Iffuint16((size%blockSize) == 0, 0, 1)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// inline if functions
func IffError(test bool, trueVal, falseVal error) error {
	if test {
		return trueVal
	}
	return falseVal
}

func IffString(test bool, trueVal, falseVal string) string {
	if test {
		return trueVal
	}
	return falseVal
}

func IffUint8(test bool, trueVal, falseVal uint8) byte {
	if test {
		return trueVal
	}
	return falseVal
}

func Iffint8(test bool, trueVal, falseVal int8) int8 {
	if test {
		return trueVal
	}
	return falseVal
}

func Iffuint16(test bool, trueVal, falseVal uint16) uint16 {
	if test {
		return trueVal
	}
	return falseVal
}

func Iffint16(test bool, trueVal, falseVal int16) int16 {
	if test {
		return trueVal
	}
	return falseVal
}

func Iffuint32(test bool, trueVal, falseVal uint32) uint32 {
	if test {
		return trueVal
	}
	return falseVal
}

func Iffint32(test bool, trueVal, falseVal int32) int32 {
	if test {
		return trueVal
	}
	return falseVal
}

func Iffuint64(test bool, trueVal, falseVal uint64) uint64 {
	if test {
		return trueVal
	}
	return falseVal
}

func Iffint64(test bool, trueVal, falseVal int64) int64 {
	if test {
		return trueVal
	}
	return falseVal
}

func Ifffloat64(test bool, trueVal, falseVal float64) float64 {
	if test {
		return trueVal
	}
	return falseVal
}
