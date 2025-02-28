package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	// Kafka library
	"github.com/segmentio/kafka-go"
	// AMQP 1.0 library
	amqp "github.com/Azure/go-amqp"
)

const (
	kafkaBrokerAddress = "127.0.0.1:9093"
	kafkaTopic         = "FOO"
	kafkaGroupID       = "demo-group"

	// AMQP configuration
	amqpBrokerAddress   = "amqp://localhost:5672"
	amqpSourceAddress   = "FOO"
	amqpExternalAddress = "EXTERNAL"
)

type SSEEvent struct {
	EventType string
	Data      string
}

var (
	kafkaUp        bool
	amqpUp         bool
	amqpExternalUp bool
	statusMu       sync.Mutex

	sseClients   []chan SSEEvent
	sseClientsMu sync.Mutex
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Start consumers and publisher
	go consumeKafka(ctx)
	go consumeAMQP(ctx)
	go runExternalAMQP(ctx)

	// Monitor brokers
	go monitorBrokerHealth(ctx)

	http.HandleFunc("/", serveIndex)
	http.HandleFunc("/sse", serveSSE)
	server := &http.Server{Addr: ":8080"}

	// Start server in a goroutine
	go func() {
		log.Println("Server running on http://localhost:8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v\n", err)
			cancel() // Cancel context if server fails
		}
	}()

	// Wait for interrupt signal
	<-sigCh
	log.Println("Received interrupt signal; shutting down gracefully...")

	// Cancel context to stop all background goroutines
	cancel()

	// Create a timeout context for graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Shutdown HTTP server
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v\n", err)
	}

	// Give some time for goroutines to clean up
	time.Sleep(2 * time.Second)
	log.Println("Server shutdown complete")
}

// ---------------------------------------------------------------------
//
//	Kafka
//
// ---------------------------------------------------------------------
func consumeKafka(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			log.Println("Kafka consumer stopping (context canceled).")
			return
		}

		log.Println("[Kafka] Attempting to connect to:", kafkaBrokerAddress)

		// Create new reader with group ID
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{kafkaBrokerAddress},
			GroupID:  kafkaGroupID,
			Topic:    kafkaTopic,
			MinBytes: 1,
			MaxBytes: 10e6,
		})

		// We'll try to fetch one message to confirm connectivity
		if err := testKafkaConnection(ctx, r); err != nil {
			log.Printf("[Kafka] Connection error: %v. Retrying...\n", err)
			statusMu.Lock()
			kafkaUp = false
			statusMu.Unlock()
			broadcastEvent(SSEEvent{EventType: "kafkaStatus", Data: boolToStatus(false)})
			_ = r.Close()
			if !sleepOrExit(ctx, 5*time.Second) {
				return
			}
			continue
		}

		// Update status to connected
		statusMu.Lock()
		kafkaUp = true
		statusMu.Unlock()
		broadcastEvent(SSEEvent{EventType: "kafkaStatus", Data: boolToStatus(true)})

		// Now read messages in a loop
		if err := readKafkaMessages(ctx, r); err != nil {
			log.Printf("[Kafka] Read loop error: %v\n", err)
			statusMu.Lock()
			kafkaUp = false
			statusMu.Unlock()
			broadcastEvent(SSEEvent{EventType: "kafkaStatus", Data: boolToStatus(false)})
		}
		_ = r.Close()

		// Reconnect after a delay, unless context canceled
		if !sleepOrExit(ctx, 5*time.Second) {
			return
		}
	}
}

func testKafkaConnection(ctx context.Context, r *kafka.Reader) error {
	// Use a longer timeout for initial connection
	testCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Get client from reader to test connection
	client := r.Config().Dialer
	if client == nil {
		client = &kafka.Dialer{
			Timeout:   1 * time.Second,
			DualStack: true,
		}
	}

	// Test connection by getting broker metadata
	conn, err := client.DialContext(testCtx, "tcp", kafkaBrokerAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %v", err)
	}
	defer conn.Close()

	// Try to get broker metadata
	_, err = conn.Brokers()
	if err != nil {
		return fmt.Errorf("failed to get broker metadata: %v", err)
	}

	return nil
}

func readKafkaMessages(ctx context.Context, r *kafka.Reader) error {
	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// normal shutdown
				return nil
			}
			return err
		}
		text := fmt.Sprintf("Kafka says: %s", string(m.Value))
		broadcastEvent(SSEEvent{EventType: "kafkaMessage", Data: text})
	}
}

// ---------------------------------------------------------------------
//
//	AMQP 1.0
//
// ---------------------------------------------------------------------
func consumeAMQP(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			log.Println("AMQP 1.0 consumer stopping (context canceled).")
			return
		}

		log.Println("[AMQP] Attempting to connect to:", amqpBrokerAddress)

		dialCtx, dialCancel := context.WithTimeout(ctx, 1*time.Second)
		conn, err := amqp.Dial(dialCtx, amqpBrokerAddress, &amqp.ConnOptions{
			SASLType: amqp.SASLTypePlain("guest", "guest"), // Default RabbitMQ credentials
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		})
		dialCancel()

		if err != nil {
			log.Printf("[AMQP] Dial error: %v. Retrying...\n", err)
			if !sleepOrExit(ctx, 1*time.Second) {
				return
			}
			continue
		}

		session, err := conn.NewSession(ctx, nil)
		if err != nil {
			log.Printf("[AMQP] Session error: %v. Retrying...\n", err)
			_ = conn.Close()
			if !sleepOrExit(ctx, 1*time.Second) {
				return
			}
			continue
		}

		// Create a receiver
		receiver, err := session.NewReceiver(ctx, amqpSourceAddress, nil)
		if err != nil {
			log.Printf("[AMQP] Receiver error: %v. Retrying...\n", err)
			_ = session.Close(ctx)
			_ = conn.Close()
			if !sleepOrExit(ctx, 1*time.Second) {
				return
			}
			continue
		}

		log.Printf("[AMQP] Connected and ready to receive messages from %s\n", amqpSourceAddress)
		amqpUp = true
		checkAndBroadcastAMQPStatus()

		// Read messages
		if err := readAMQPMessages(ctx, receiver); err != nil {
			log.Printf("[AMQP] Error reading messages: %v\n", err)
			_ = receiver.Close(ctx)
			_ = session.Close(ctx)
			_ = conn.Close()
			amqpUp = false
			checkAndBroadcastAMQPStatus()
			if !sleepOrExit(ctx, 5*time.Second) {
				return
			}
			continue
		}
	}
}

func readAMQPMessages(ctx context.Context, receiver *amqp.Receiver) error {
	for {
		msg, err := receiver.Receive(ctx, nil)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("amqp receive error: %w", err)
		}

		if err := receiver.AcceptMessage(ctx, msg); err != nil {
			return fmt.Errorf("amqp accept error: %w", err)
		}

		text := fmt.Sprintf("AMQP says: %s", string(msg.GetData()))
		broadcastEvent(SSEEvent{EventType: "amqpMessage", Data: text})
	}
}

// ---------------------------------------------------------------------
//
//	External AMQP Block
//
// ---------------------------------------------------------------------

func runExternalAMQP(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			log.Println("External AMQP stopping (context canceled).")
			return
		}

		log.Println("[External AMQP] Attempting to connect to:", amqpBrokerAddress)

		dialCtx, dialCancel := context.WithTimeout(ctx, 5*time.Second)
		conn, err := amqp.Dial(dialCtx, amqpBrokerAddress, &amqp.ConnOptions{
			SASLType: amqp.SASLTypePlain("guest", "guest"),
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		})
		dialCancel()

		if err != nil {
			log.Printf("[External AMQP] Dial error: %v. Retrying...\n", err)
			setExternalAMQPStatus(false)
			if !sleepOrExit(ctx, 5*time.Second) {
				return
			}
			continue
		}

		// Create a session
		session, err := conn.NewSession(ctx, nil)
		if err != nil {
			log.Printf("[External AMQP] Session error: %v. Retrying...\n", err)
			conn.Close()
			setExternalAMQPStatus(false)
			if !sleepOrExit(ctx, 5*time.Second) {
				return
			}
			continue
		}

		// Create a sender
		sender, err := session.NewSender(ctx, amqpExternalAddress, nil)
		if err != nil {
			log.Printf("[External AMQP] Sender creation error: %v. Retrying...\n", err)
			session.Close(context.Background())
			conn.Close()
			setExternalAMQPStatus(false)
			if !sleepOrExit(ctx, 5*time.Second) {
				return
			}
			continue
		}

		setExternalAMQPStatus(true)

		// Start sending messages
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				sender.Close(context.Background())
				session.Close(context.Background())
				conn.Close()
				return
			case t := <-ticker.C:
				msg := fmt.Sprintf("External message at %s", t.Format(time.RFC3339))
				err := sender.Send(ctx, amqp.NewMessage([]byte(msg)), nil)
				if err != nil {
					log.Printf("[External AMQP] Send error: %v\n", err)
					sender.Close(context.Background())
					session.Close(context.Background())
					conn.Close()
					setExternalAMQPStatus(false)
					break
				}
				// Broadcast the sent message to UI
				broadcastEvent(SSEEvent{EventType: "externalMessage", Data: fmt.Sprintf("External sent: %s", msg)})
			}
		}
	}
}

func setExternalAMQPStatus(up bool) {
	statusMu.Lock()
	defer statusMu.Unlock()
	if amqpExternalUp != up {
		amqpExternalUp = up
		broadcastEvent(SSEEvent{
			EventType: "externalStatus",
			Data:      boolToStatus(up),
		})
	}
}

// ---------------------------------------------------------------------
//
//	Broker Health Check
//
// ---------------------------------------------------------------------
func monitorBrokerHealth(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Broker health monitor stopped.")
			return
		case <-ticker.C:
			checkAndBroadcastKafkaStatus()
			checkAndBroadcastAMQPStatus()
			checkAndBroadcastExternalStatus()
		}
	}
}

func checkAndBroadcastKafkaStatus() {
	newStatus := checkKafka()
	statusMu.Lock()
	changed := (newStatus != kafkaUp)
	kafkaUp = newStatus
	statusMu.Unlock()
	if changed {
		broadcastEvent(SSEEvent{EventType: "kafkaStatus", Data: boolToStatus(newStatus)})
	}
}

func checkKafka() bool {
	dialCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	dialer := &kafka.Dialer{
		Timeout:   1 * time.Second,
		DualStack: true,
	}

	// Try to establish a direct connection first
	conn, err := dialer.DialContext(dialCtx, "tcp", kafkaBrokerAddress)
	if err != nil {
		log.Printf("[Kafka Health] Connection error: %v\n", err)
		return false
	}
	defer conn.Close()

	// Try to get broker metadata
	_, err = conn.Brokers()
	if err != nil {
		log.Printf("[Kafka Health] Failed to get broker metadata: %v\n", err)
		return false
	}

	return true
}

func checkAndBroadcastAMQPStatus() {
	newStatus := checkAMQP()
	statusMu.Lock()
	changed := (newStatus != amqpUp)
	amqpUp = newStatus
	statusMu.Unlock()
	if changed {
		broadcastEvent(SSEEvent{EventType: "amqpStatus", Data: boolToStatus(newStatus)})
	}
}

func checkAMQP() bool {
	dialCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Use the same connection options as the main consumer
	conn, err := amqp.Dial(dialCtx, amqpBrokerAddress, &amqp.ConnOptions{
		SASLType: amqp.SASLTypePlain("guest", "guest"),
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	})
	if err != nil {
		return false
	}
	defer conn.Close()

	// Try to create a session to verify AMQP is truly available
	session, err := conn.NewSession(dialCtx, nil)
	if err != nil {
		return false
	}
	defer session.Close(dialCtx)

	return true
}

func checkAndBroadcastExternalStatus() {
	statusMu.Lock()
	currentStatus := amqpExternalUp
	statusMu.Unlock()

	newStatus := checkExternalAMQP()
	if currentStatus != newStatus {
		setExternalAMQPStatus(newStatus)
	}
}

func checkExternalAMQP() bool {
	dialCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := amqp.Dial(dialCtx, amqpBrokerAddress, &amqp.ConnOptions{
		SASLType: amqp.SASLTypePlain("guest", "guest"),
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	})
	if err != nil {
		return false
	}
	defer conn.Close()

	session, err := conn.NewSession(context.Background(), nil)
	if err != nil {
		return false
	}
	defer session.Close(context.Background())

	sender, err := session.NewSender(context.Background(), amqpExternalAddress, nil)
	if err != nil {
		return false
	}
	defer sender.Close(context.Background())

	return true
}

func boolToStatus(up bool) string {
	if up {
		return "UP"
	}
	return "DOWN"
}

// ---------------------------------------------------------------------
//
//	SSE & Helpers
//
// ---------------------------------------------------------------------
func broadcastEvent(event SSEEvent) {
	sseClientsMu.Lock()
	defer sseClientsMu.Unlock()
	for _, ch := range sseClients {
		select {
		case ch <- event:
		default:
		}
	}
}

func serveSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	clientCh := make(chan SSEEvent, 10)

	sseClientsMu.Lock()
	sseClients = append(sseClients, clientCh)
	sseClientsMu.Unlock()

	defer func() {
		sseClientsMu.Lock()
		for i, ch := range sseClients {
			if ch == clientCh {
				sseClients = append(sseClients[:i], sseClients[i+1:]...)
				break
			}
		}
		sseClientsMu.Unlock()
		close(clientCh)
	}()

	// send current known statuses
	statusMu.Lock()
	kafkaStatus := boolToStatus(kafkaUp)
	amqpStatus := boolToStatus(amqpUp)
	amqpExternalStatus := boolToStatus(amqpExternalUp)
	statusMu.Unlock()

	fmt.Fprintf(w, "event: kafkaStatus\ndata: %s\n\n", kafkaStatus)
	fmt.Fprintf(w, "event: amqpStatus\ndata: %s\n\n", amqpStatus)
	fmt.Fprintf(w, "event: externalStatus\ndata: %s\n\n", amqpExternalStatus)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	for {
		select {
		case event, ok := <-clientCh:
			if !ok {
				return
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.EventType, event.Data)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		case <-r.Context().Done():
			return
		}
	}
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, indexHTML)
}

// Sleep or exit if context is canceled.
func sleepOrExit(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

// ---------------------------------------------------------------------
//
//	HTML
//
// ---------------------------------------------------------------------
var indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Golang Demo - Kafka & AMQP 1.0</title>
<script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-100 p-4">
  <div class="max-w-3xl mx-auto">
    <h1 class="text-3xl font-bold mb-4">Infrastructure Demo</h1>
    <p class="mb-4 text-gray-700">Check logs for connection retries & statuses.</p>

    <!-- Production Block (Kafka) -->
    <div id="production" class="p-4 mb-4 border-2 border-gray-300 rounded-md">
      <h2 class="text-xl font-semibold mb-2">Production</h2>
      <div id="kafkaBlock" class="p-4 mb-2 rounded-md border-2 border-gray-300">
        <h3 class="text-lg font-medium">Kafka</h3>
        <div id="kafkaStatus" class="text-sm mt-1">Checking status...</div>
        <div id="kafkaMessages" class="mt-2 text-sm"></div>
      </div>
    </div>

    <!-- Backup Block (AMQP 1.0) -->
    <div id="backup" class="p-4 mb-4 border-2 border-gray-300 rounded-md">
      <h2 class="text-xl font-semibold mb-2">Backup</h2>
      <div id="amqpBlock" class="p-4 mb-2 rounded-md border-2 border-gray-300">
        <h3 class="text-lg font-medium">AMQP 1.0</h3>
        <div id="amqpStatus" class="text-sm mt-1">Checking status...</div>
        <div id="amqpMessages" class="mt-2 text-sm"></div>
      </div>
    </div>

    <!-- External Block (AMQP 1.0) -->
    <div id="external" class="p-4 mb-4 border-2 border-gray-300 rounded-md">
      <h2 class="text-xl font-semibold mb-2">External</h2>
      <div id="amqpExternalBlock" class="p-4 mb-2 rounded-md border-2 border-gray-300">
        <h3 class="text-lg font-medium">AMQP 1.0</h3>
        <div id="amqpExternalStatus" class="text-sm mt-1">Checking status...</div>
        <div id="amqpExternalMessages" class="mt-2 text-sm"></div>
      </div>
    </div>
  </div>

<script>
  const evtSource = new EventSource('/sse');
  const kafkaStatusEl = document.getElementById('kafkaStatus');
  const amqpStatusEl = document.getElementById('amqpStatus');
  const kafkaMessagesEl = document.getElementById('kafkaMessages');
  const amqpMessagesEl = document.getElementById('amqpMessages');
  const kafkaBlockEl = document.getElementById('kafkaBlock');
  const amqpBlockEl = document.getElementById('amqpBlock');
  const amqpExternalStatusEl = document.getElementById('amqpExternalStatus');
  const amqpExternalMessagesEl = document.getElementById('amqpExternalMessages');
  const amqpExternalBlockEl = document.getElementById('amqpExternalBlock');

  // Function to limit messages in a container
  function limitMessages(container, maxMessages = 5) {
    while (container.children.length > maxMessages) {
      container.removeChild(container.lastChild);
    }
  }

  evtSource.addEventListener('kafkaStatus', e => {
    const status = e.data;
    kafkaStatusEl.textContent = 'Status: ' + status;
    if (status === 'UP') {
      kafkaBlockEl.classList.remove('border-red-500');
      kafkaBlockEl.classList.add('border-green-500');
    } else {
      kafkaBlockEl.classList.remove('border-green-500');
      kafkaBlockEl.classList.add('border-red-500');
    }
  });

  evtSource.addEventListener('amqpStatus', e => {
    const status = e.data;
    amqpStatusEl.textContent = 'Status: ' + status;
    if (status === 'UP') {
      amqpBlockEl.classList.remove('border-red-500');
      amqpBlockEl.classList.add('border-green-500');
    } else {
      amqpBlockEl.classList.remove('border-green-500');
      amqpBlockEl.classList.add('border-red-500');
    }
  });

  evtSource.addEventListener('externalStatus', e => {
    const status = e.data;
    amqpExternalStatusEl.textContent = 'Status: ' + status;
    if (status === 'UP') {
      amqpExternalBlockEl.classList.remove('border-red-500');
      amqpExternalBlockEl.classList.add('border-green-500');
    } else {
      amqpExternalBlockEl.classList.remove('border-green-500');
      amqpExternalBlockEl.classList.add('border-red-500');
    }
  });

  evtSource.addEventListener('kafkaMessage', e => {
    const div = document.createElement('div');
    div.textContent = e.data;
    kafkaMessagesEl.prepend(div);
    limitMessages(kafkaMessagesEl);
  });

  evtSource.addEventListener('amqpMessage', e => {
    const div = document.createElement('div');
    div.textContent = e.data;
    amqpMessagesEl.prepend(div);
    limitMessages(amqpMessagesEl);
  });

  evtSource.addEventListener('externalMessage', e => {
    const div = document.createElement('div');
    div.textContent = e.data;
    amqpExternalMessagesEl.prepend(div);
    limitMessages(amqpExternalMessagesEl);
  });

  evtSource.onerror = e => {
    console.error('EventSource error:', e);
  };
</script>
</body>
</html>
`
