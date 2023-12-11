package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/aws/aws-lambda-go/lambda"
)

// https://github.com/memphisdev/memphis.go#creating-a-memphis-function
func MyEventHandler(message []byte, headers map[string]string, inputs map[string]string) ([]byte, map[string]string,  error){
	// Here is a short example of converting the message payload to bytes and back

	var event map[string]interface{}
	json.Unmarshal(message, &event)
	event["Testing"] = "Working"
	
	// Return the payload back as []bytes
	eventBytes, _ := json.Marshal(event)
	return eventBytes, headers, nil
}

func MyEventHandlerSchema(message interface{}, headers map[string]string, inputs map[string]string) (interface{}, map[string]string,  error){
	typedMessage, ok := message.(Data)
	if !ok{
		return nil, nil, fmt.Errorf("Data could not be asserted bruhsky: %v, %v", message, reflect.TypeOf(message))
	}

	typedMessage.MyData = "This new data is cool"

	return message, headers, nil
}

type Data struct{
	MyData string `json:"my_data"`
}

func main() {	
	var data Data
	CreateFunction(EventHandlerSchemaOption(MyEventHandlerSchema, &data))
}


type MemphisMsg struct {
	Headers map[string]string `json:"headers"`
	Payload string            `json:"payload"`
}

type MemphisMsgWithError struct {
	Headers map[string]string `json:"headers"`
	Payload string            `json:"payload"`
	Error   string            `json:"error"`
}

type MemphisEvent struct {
	Inputs   map[string]string `json:"inputs"`
	Messages []MemphisMsg      `json:"messages"`
}

type MemphisOutput struct {
	Messages       []MemphisMsg          `json:"messages"`
	FailedMessages []MemphisMsgWithError `json:"failed_messages"`
}

// EventHandlerFunction gets the message payload as []byte, message headers as map[string]string and inputs as map[string]string and should return the modified payload and headers.
// error should be returned if the message should be considered failed and go into the dead-letter station.
// if all returned values are nil the message will be filtered out of the station.
type EventHandler func([]byte, map[string]string, map[string]string) ([]byte, map[string]string, error)
type HandlerWithSchema func(interface{}, map[string]string, map[string]string) (interface{}, map[string]string, error)

type HandlerOption func(*HandlerOptions) error

type HandlerOptions struct {
	Handler           EventHandler
	HandlerWithSchema HandlerWithSchema
	UserObject        interface{}
}

func EventHandlerOption(handler EventHandler) HandlerOption {
	return func(handlerOptions *HandlerOptions) error {
		handlerOptions.Handler = handler
		return nil
	}
}

func EventHandlerSchemaOption(handler HandlerWithSchema, schema interface{}) HandlerOption {
	return func(handlerOptions *HandlerOptions) error {
		handlerOptions.HandlerWithSchema = handler
		handlerOptions.UserObject = schema
		return nil
	}
}

func UnmarshalIntoStruct(data []byte, userStruct interface{}) error {
	// Unmarshal JSON data into the struct
	err := json.Unmarshal(data, userStruct)
	if err != nil {
		return err
	}

	fmt.Println(userStruct)
	fmt.Println(reflect.TypeOf(userStruct))

	return nil
}

func checkValidStruct(userStruct interface{}) error {
	// Check if the provided variable is a pointer to a struct
	valueOf := reflect.ValueOf(userStruct)
	if valueOf.Kind() != reflect.Ptr || valueOf.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("input parameter must be a pointer to a struct")
	}

	return nil
}

// This function creates a Memphis function and processes events with the passed-in eventHandler function.
// eventHandlerFunction gets the message payload as []byte, message headers as map[string]string and inputs as map[string]string and should return the modified payload and headers.
// error should be returned if the message should be considered failed and go into the dead-letter station.
// if all returned values are nil the message will be filtered out from the station.
func CreateFunction(options ...HandlerOption) {
	LambdaHandler := func(ctx context.Context, event *MemphisEvent) (*MemphisOutput, error) {
		params := HandlerOptions{
			Handler:           nil,
			HandlerWithSchema: nil,
			UserObject:        nil,
		}

		for _, option := range options {
			if option != nil {
				if err := option(&params); err != nil {
					return nil, err
				}
			}
		}

		if params.UserObject != nil {
			err := checkValidStruct(params.UserObject)
			if err != nil {
				return nil, err
			}
		}

		if len(options) == 0 {
			return nil, fmt.Errorf("the user must pass in at least one option containing the handler function, or the handler with schema function and its schema")
		} else if len(options) > 1 {
			return nil, fmt.Errorf("the user passed in too many options. Functions only supports giivng one option")
		}

		var processedEvent MemphisOutput
		for _, msg := range event.Messages {
			payload, err := base64.StdEncoding.DecodeString(msg.Payload)
			if err != nil {
				processedEvent.FailedMessages = append(processedEvent.FailedMessages, MemphisMsgWithError{
					Headers: msg.Headers,
					Payload: msg.Payload,
					Error:   "couldn't decode message: " + err.Error(),
				})
				continue
			}

			var modifiedPayload interface{}
			var modifiedHeaders map[string]string
			if params.UserObject == nil {
				modifiedPayload, modifiedHeaders, err = params.Handler(payload, msg.Headers, event.Inputs)
			} else {
				UnmarshalIntoStruct(payload, params.UserObject)
				var tmpPayload interface{}
				tmpPayload, modifiedHeaders, err = params.HandlerWithSchema(params.UserObject, msg.Headers, event.Inputs) // err will proagate to next if
				if err == nil {
					modifiedPayload, err = json.Marshal(tmpPayload) // err will proagate to next if
				}
			}

			if err != nil {
				processedEvent.FailedMessages = append(processedEvent.FailedMessages, MemphisMsgWithError{
					Headers: msg.Headers,
					Payload: msg.Payload,
					Error:   err.Error(),
				})
				continue
			}

			if modifiedPayload != nil && modifiedHeaders != nil {
				modifiedPayloadStr := base64.StdEncoding.EncodeToString(modifiedPayload.([]byte))
				processedEvent.Messages = append(processedEvent.Messages, MemphisMsg{
					Headers: modifiedHeaders,
					Payload: modifiedPayloadStr,
				})
			}
		}

		return &processedEvent, nil
	}

	lambda.Start(LambdaHandler)
}
