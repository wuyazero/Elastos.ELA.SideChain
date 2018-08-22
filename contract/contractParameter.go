package contract

//parameter defined type.
type ContractParameterType byte

const (
	Signature ContractParameterType = iota
	Boolean
	Integer
	Hash160
	Hash256
	ByteArray
	PublicKey
	String
	Object
	Array = 0x10
	Void  = 0xff
)

var ParameterTypeMap = map[string]ContractParameterType{
	"Signature": Signature,
	"Boolean":   Boolean,
	"Integer":   Integer,
	"Hash160":   Hash160,
	"Hash256":   Hash256,
	"ByteArray": ByteArray,
	"PublicKey": PublicKey,
	"String":    String,
	"Object":    Object,
	"Array":     Array,
	"Void":      Void,
}
