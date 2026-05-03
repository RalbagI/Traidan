import { useState, useEffect } from 'react';
import OrderBookCanvas from './OrderBookCanvas';
import WebSocketClient from './WebSocketClient';

export default function App() {
  const [marketData, setMarketData] = useState(null);
  const [aiSignal, setAiSignal] = useState(null);

  useEffect(() => {
    const ws = new WebSocketClient('ws://localhost:8080/stream', (msg) => {
      if (msg.type === 'book_update') setMarketData(msg.data);
      if (msg.type === 'ai_signal')   setAiSignal(msg.data);
    });
    return () => ws.close();
  }, []);

  return (
    <div style={{ background: '#0d1117', color: '#e6edf3', minHeight: '100vh', padding: '1rem' }}>
      <h1 style={{ fontSize: '1.1rem', marginBottom: '1rem' }}>Traderalbag — Live Trading Dashboard</h1>
      <OrderBookCanvas bookData={marketData} signal={aiSignal} />
    </div>
  );
}
