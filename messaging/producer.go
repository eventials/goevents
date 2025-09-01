package messaging

type MessageInput struct {
	// Action represents the type of action or event to be published.
	Action string
	// Data contains the message payload as a byte slice.
	Data []byte
	// MessageGroupID is used to group messages in SNS FIFO topics.
	// Currently, this field only has effect when the destination is SNS and is recommended for FIFO topics,
	// as it ensures message ordering within the same group.
	MessageGroupID *string
}
type Producer interface {
	Publish(input MessageInput)
	NotifyClose() <-chan bool
	Close()
}
