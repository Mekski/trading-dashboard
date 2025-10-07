# Trading Dashboard - Portfolio Demo

A modern algorithmic trading dashboard showcasing portfolio performance visualization with realistic simulated market data.

## 🚀 Features

- **Modern Glassmorphic UI**: Beautiful dark-themed interface with frosted glass effects and animated backgrounds
- **Real-time Performance Tracking**: Monitor 12 trading strategies across 4 cryptocurrency pairs (BTC, ETH, SOL, LTC)
- **Interactive Charts**: Zoomable Plotly.js visualizations with multiple time range selections
- **Top Performers Section**: Identify gainers, losers, and trending strategies at a glance
- **Responsive Design**: Seamless experience across desktop and mobile devices
- **Theme Customization**: Light/dark modes with multiple accent colors

## 💻 Tech Stack

- **Backend**: Flask (Python 3.7+)
- **Frontend**: Vanilla JavaScript, HTML5, CSS3 with modern glassmorphic design
- **Charts**: Plotly.js for interactive financial visualizations
- **Data Processing**: Pandas & NumPy for complex financial calculations

## 🌐 Live Demo

Visit the deployed version on Vercel: [Coming Soon]

## 📦 Deployment

### Deploy to Vercel (Recommended)

[![Deploy with Vercel](https://vercel.com/button)](https://vercel.com/new/clone?repository-url=https://github.com/yourusername/trading-dashboard)

### Manual Deployment

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/trading-dashboard.git
cd trading-dashboard
```

2. **Deploy with Vercel CLI**
```bash
npm i -g vercel
vercel
```

3. **Follow the prompts** to link your Vercel account

### Alternative: Deploy via GitHub

1. Push code to GitHub repository
2. Import project in [Vercel Dashboard](https://vercel.com/dashboard)
3. Vercel auto-detects Flask configuration and deploys

## 🔧 Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run the application
python web_app.py

# Open browser to http://localhost:5001
```

## 📊 Dashboard Features

### Overview Tab
- Aggregate portfolio performance across all strategies
- Real-time statistics: freshness, positions, returns, win/loss ratios
- Sortable data table with search functionality

### Top Performers Tab
- **Top Gainers**: Highest cumulative returns
- **24h Stars**: Best daily performers
- **Hot Streaks**: Consistent winners
- **Trending Up**: Positive momentum strategies
- **Wide Range**: Most volatile strategies
- **Biggest Losers**: Underperforming strategies

### Individual Symbol View
- Detailed price action with position overlays
- Cumulative returns tracking
- Minutely and hourly chart resolutions
- Synchronized zoom across multiple charts

## 🎨 UI Customization

The dashboard includes a floating theme switcher with:
- **Light/Dark Mode Toggle**
- **12 Accent Colors**: Indigo, Purple, Green, Orange, Pink, Blue, Red, Teal, Amber, Rose, Emerald
- **Persistent Settings**: Theme preferences saved across sessions

## 📈 Data

This demo uses realistic simulated trading data that showcases:
- Natural market volatility with sharp fluctuations
- Position-based returns (long/short only, no flat positions)
- Transaction cost modeling (0.05% fees)
- Diverse performance profiles (±90% range with rare outliers)

## 🏗️ Architecture

```
Flask Backend → API Endpoints → JSON Data
       ↓              ↓              ↓
   Web Server    RESTful API    CSV Processing
       ↓              ↓              ↓
   HTML/CSS      JavaScript      Plotly Charts
```

## 📝 License

MIT License - Feel free to use this project in your portfolio

## 🤝 Contributing

This is a portfolio demonstration project. Feel free to fork and customize for your own use!

## 📧 Contact

For questions or opportunities, please reach out through my portfolio website.

---

Built with ❤️ for demonstrating full-stack development and financial data visualization capabilities.