package oramapper

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/grpclog"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"gopkg.in/oleiade/reflections.v1"
	ora "gopkg.in/rana/ora.v4"
)

// Mapper is the struct receiver for the package.
type Mapper struct {
	SourceMap    map[string]int
	TagMap       map[string]string
	TargetMap    map[string]reflect.StructField
	TargetStruct *interface{}
	LastTarget   string
}

// New is the initialization for the methods.
func New() (*Mapper, error) {
	mapper := Mapper{
		SourceMap: make(map[string]int),
		TagMap:    make(map[string]string),
		TargetMap: make(map[string]reflect.StructField),
	}
	return &mapper, nil
}

func (m *Mapper) SetTarget(target interface{}) error {
	// Third is to extract the fields and structfields
	// Fourth is to set the fieldmap.

	// First check is to see if the target passed is a valid type for our purposes.
	if !isValidType(target) {
		return errors.New("Invalid target type")
	}

	// Second check is to get a real copy of the target in the case that it is a pointer.
	targetValue := reflectValue(target)

	targetType := targetValue.Type()

	if m.LastTarget == targetType.Name() {
		return nil
	}

	m.LastTarget = targetType.Name()

	targetFieldCount := targetType.NumField()

	for i := 0; i < targetFieldCount; i++ {
		field := targetType.Field(i)
		m.TargetMap[strings.ToLower(field.Name)] = field
	}

	return nil
}

// Setup your source.  Run immediately after you know the result set is open.
func (m *Mapper) SetSource(columns []ora.Column) error {
	for k, v := range columns {
		m.SourceMap[strings.ToLower(v.Name)] = k
	}

	return nil
}

func (m *Mapper) MapStruct(row []interface{}, target interface{}) error {

	// For each item we have in the row, look it up in the source map.

	err := m.SetTarget(target)
	if err != nil {
		return errors.New(err.Error())
	}

	for k, v := range m.SourceMap {
		// Need to see if we have a map in the tags map.  If we do, use that.
		// If we do not, then need to see if we have a map in the target map.  If we do, use that.
		// If we do not have a map anywhere, then we do not do anything.
		// grpclog.Println("Working on", k)
		targetField, err := m.GetTargetField(k)
		if err != nil {
			grpclog.Println(err)
			continue
		}
		r, err := ValueToType(row[v], targetField.Type.Name())
		if err != nil {
			grpclog.Println(err)
			continue
		}
		// fmt.Printf("%v\n%v\n%v\n", target, targetField.Name, r)
		err = reflections.SetField(target, targetField.Name, r)

	}

	return nil
}

func (m Mapper) GetTargetField(key string) (result reflect.StructField, err error) {
	// First find in the tags.
	// If found, return that.

	ok := false

	if innerResult, ok := m.TagMap[key]; ok {
		result = m.TargetMap[innerResult]
		// grpclog.Println("Found a tag match for", key, "and it is", innerResult)
		// grpclog.Println("\t\twhich is ", result.Name)
		return
	}

	if result, ok = m.TargetMap[key]; ok {
		// grpclog.Println("Found a name match for", key, "and it is", result.Name)
		return
	}

	return reflect.StructField{}, errors.New("unable to map struct field")

}

func reflectValue(target interface{}) reflect.Value {
	reflectedType := reflect.TypeOf(target).Kind()
	switch reflectedType {
	case reflect.Ptr:
		return reflect.ValueOf(target).Elem()
	default:
		return reflect.ValueOf(target)
	}
}

func isValidType(target interface{}) bool {
	for _, t := range validTypes() {
		if reflect.TypeOf(target).Kind() == t {
			return true
		}
	}
	return false
}

func validTypes() []reflect.Kind {
	return []reflect.Kind{reflect.Struct, reflect.Ptr}
}

func ValueToType(value interface{}, outputType string) (result interface{}, err error) {

	if value == nil {
		err = ErrNilValue("ValueToType")
		return
	}

	switch outputType {
	case "int64":
		result, err = RowValueToInt64(value)
		return result, err
	case "int32":
		result, err = RowValueToInt32(value)
		return result, err
	case "string":
		result, err = RowValueToString(value)
		return result, err
	case "*time.Timestamp":
		result, err = RowValueToTimestamp(value)
		return result, err
	}
	// fmt.Printf("outputType is %s\n", outputType)
	err = ErrWhatIsThis("ValueToType", value)
	return
}

// RowValueToTimestamp is a function, now shut up.
func RowValueToTimestamp(value interface{}) (result *timestamp.Timestamp, err error) {
	result, err = ptypes.TimestampProto(value.(time.Time))
	if err != nil {
		err = ErrWhatIsThis("RowValueToInt32", value)
		return
	}
	return
}

// RowValueToInt64 will attempt to convert the provided value to an int64
func RowValueToInt64(value interface{}) (result int64, err error) {
	// First test for int64
	if newvar, ok := value.(int64); ok {
		result = newvar
		return
	}

	// First test for OCINum
	if newvar, ok := value.(ora.OCINum); ok {
		result, err = strconv.ParseInt(newvar.String(), 10, 64)
		return
	}

	// No joy
	result = 0
	err = ErrWhatIsThis("RowValueToInt32", value)
	return

}

// RowValueToInt32 will attempt to convert the provided value to an int32
func RowValueToInt32(value interface{}) (result int32, err error) {
	innerResult, err := RowValueToInt64(value)
	if err != nil {
		err = ErrWhatIsThis("RowValueToInt32", value)
		return
	}

	result = int32(innerResult)
	return
}

// RowValueToString will attempt to convert the provided value into a string.
func RowValueToString(value interface{}) (result string, err error) {
	result = value.(string)
	return
}

// ErrWhatIsThis is a handy error to be returned when we do not know how to deal with a specific type fo entity.
func ErrWhatIsThis(scope string, entity interface{}) error {
	return fmt.Errorf("%s: Unknown type \"%s\"", scope, reflect.TypeOf(entity))
}

// ErrNilValue is a handy error to be returned when someone asks us to work on a provided nil value.
func ErrNilValue(scope string) error {
	return fmt.Errorf("%s: Nil value provided", scope)
}
