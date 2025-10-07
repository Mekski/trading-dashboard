/* Modern Dashboard JavaScript
 * Extracted from modern.html for better organization and caching
 */

// Global variables
let allData = null;

// Helper function to safely update element content
function safeUpdateElement(id, property, value) {
    const element = document.getElementById(id);
    if (element) {
        element[property] = value;
    }
}
let symbolsData = [];
let portfolioChartData = null;
let currentTimeRange = 'ALL';
let currentView = 'overview';
let currentCoin = 'all';
let useLogScale = false;
let autoRefreshInterval = null;
let syncCountdownInterval = null;
let lastSyncTime = null;
let searchTerm = '';
let chartVisible = true;
let sortColumn = null;
let sortDirection = 'asc'; // 'asc' or 'desc'
let previousValues = {}; // Store previous values for animation comparison
let currentAccent = 'default';

// Format return with color
function formatReturn(value) {
    const sign = value >= 0 ? '+' : '';
    const className = value >= 0 ? 'positive' : 'negative';
    return `<span class="return-value ${className}">${sign}${value.toFixed(2)}%</span>`;
}

// Format position badge
function formatPosition(position) {
    const posClass = position.toLowerCase();
    return `<span class="position-badge ${posClass}">${position}</span>`;
}

// Format freshness
function formatFreshness(freshness, minutesAgo) {
    const freshClass = freshness.replace('_', '-');
    const timeStr = minutesAgo < 60 ? `${minutesAgo}m` : 
                  `${Math.floor(minutesAgo / 60)}h ${minutesAgo % 60}m`;
    return `
        <span class="freshness">
            <span class="freshness-dot ${freshClass}"></span>
            <span>${timeStr}</span>
        </span>
    `;
}

// Animate counter from start to end
function animateCounter(element, start, end, duration = 1000, suffix = '') {
    const range = end - start;
    const increment = range / (duration / 16); // 60 FPS
    let current = start;
    
    const timer = setInterval(() => {
        current += increment;
        if ((increment > 0 && current >= end) || (increment < 0 && current <= end)) {
            current = end;
            clearInterval(timer);
        }
        
        if (suffix.includes('%')) {
            element.textContent = current.toFixed(2) + suffix;
        } else {
            element.textContent = Math.round(current) + suffix;
        }
    }, 16);
}

// Helper function to update text while preserving arrows
function updateStatText(element, text) {
    if (!element) return;
    
    // Save existing arrow if present
    const arrow = element.querySelector('.value-arrow');
    
    // Update text content
    element.textContent = text;
    
    // Re-append arrow if it existed
    if (arrow) {
        element.appendChild(arrow);
    }
}

// Add directional arrow indicator
function addDirectionalArrow(elementId, oldValue, newValue) {
    const element = document.getElementById(elementId);
    if (!element) return;
    
    // Remove any existing arrow from element itself
    const existing = element.querySelector('.value-arrow');
    if (existing) existing.remove();
    
    if (oldValue !== undefined && oldValue !== null && newValue !== oldValue) {
        const arrow = document.createElement('span');
        arrow.className = 'value-arrow';
        
        if (newValue > oldValue) {
            arrow.className += ' up';
            arrow.innerHTML = '↑';
        } else if (newValue < oldValue) {
            arrow.className += ' down';
            arrow.innerHTML = '↓';
        }
        
        // Append arrow to the value element (inline)
        element.appendChild(arrow);
    }
}

// Update statistics based on selected coin
function updateStatistics(coin = 'all') {
    if (!allData) return;
    
    // Check if we're on a page with stats elements
    if (!document.getElementById('avgReturn')) return;
    
    let stats, symbols;
    if (coin === 'all') {
        stats = allData.stats;
        symbols = allData.symbols;
    } else {
        stats = allData.coin_stats[coin.toUpperCase()];
        symbols = allData.symbols.filter(s => s.symbol === coin.toUpperCase());
        if (!stats) {
            stats = {
                total_symbols: 0,
                fresh_symbols: 0,
                freshness_percent: 0,
                avg_return: 0,
                min_return: 0,
                max_return: 0,
                median_return: 0,
                positive_cumulative: 0,
                negative_cumulative: 0,
                positive_24h: 0,
                negative_24h: 0,
                active_positions: 0
            };
        }
    }
    
    // Animate value changes with smooth transitions
    const animateValue = (elementId, newValue, isNumeric = false, suffix = '') => {
        const element = document.getElementById(elementId);
        if (!element) return;
        
        const key = `${elementId}_${coin}`;
        const oldValue = previousValues[key];
        
        if (isNumeric && oldValue !== undefined && oldValue !== newValue) {
            element.classList.add('value-pulse');
            animateCounter(element, oldValue, newValue, 800, suffix);
            setTimeout(() => element.classList.remove('value-pulse'), 600);
        } else {
            element.textContent = newValue;
        }
        
        previousValues[key] = newValue;
    };
    
    // Update freshness with animated counters
    const freshnessKey = `freshness_${coin}`;
    const oldFresh = previousValues[freshnessKey];
    const element = document.getElementById('freshnessStat');
    
    if (oldFresh !== undefined && oldFresh !== stats.fresh_symbols) {
        element.classList.add('value-pulse');
        setTimeout(() => element.classList.remove('value-pulse'), 600);
    }
    
    updateStatText(element, `${stats.fresh_symbols}/${stats.total_symbols}`);
    addDirectionalArrow('freshnessStat', oldFresh, stats.fresh_symbols);
    previousValues[freshnessKey] = stats.fresh_symbols;
    
    safeUpdateElement('freshnessPercent', 'textContent', `${stats.freshness_percent}% up-to-date`);
    
    // Update average return with animation
    const avgReturnKey = `avgReturn_${coin}`;
    const oldAvgReturn = previousValues[avgReturnKey];
    const avgReturnEl = document.getElementById('avgReturn');
    if (!avgReturnEl) return; // Exit if element doesn't exist
    
    if (oldAvgReturn !== undefined && Math.abs(oldAvgReturn - stats.avg_return) > 0.01) {
        avgReturnEl.classList.add('value-pulse');
        animateCounter(avgReturnEl, oldAvgReturn, stats.avg_return, 800, '%');
        setTimeout(() => {
            avgReturnEl.classList.remove('value-pulse');
            avgReturnEl.innerHTML = formatReturn(stats.avg_return);
            addDirectionalArrow('avgReturn', oldAvgReturn, stats.avg_return);
        }, 900);
    } else {
        avgReturnEl.innerHTML = formatReturn(stats.avg_return);
        if (oldAvgReturn !== undefined) {
            addDirectionalArrow('avgReturn', oldAvgReturn, stats.avg_return);
        }
    }
    previousValues[avgReturnKey] = stats.avg_return;
    
    if (stats.min_return !== undefined && stats.max_return !== undefined) {
        document.getElementById('returnRange').textContent = 
            `${stats.min_return.toFixed(1)}% to ${stats.max_return.toFixed(1)}%`;
    }
    
    // Update active positions with animation
    if (coin === 'all' || symbols.length > 0) {
        const positions = symbols.reduce((acc, symbol) => {
            acc[symbol.position] = (acc[symbol.position] || 0) + 1;
            return acc;
        }, {});
        const activeCount = (positions.LONG || 0) + (positions.SHORT || 0);
        
        const activeKey = `active_${coin}`;
        const oldActive = previousValues[activeKey];
        const activeEl = document.getElementById('activePositions');
        
        if (oldActive !== undefined && oldActive !== activeCount) {
            activeEl.classList.add('value-pulse');
            setTimeout(() => activeEl.classList.remove('value-pulse'), 600);
        }
        
        updateStatText(activeEl, `${activeCount}/${stats.total_symbols}`);
        addDirectionalArrow('activePositions', oldActive, activeCount);
        previousValues[activeKey] = activeCount;
        
        document.getElementById('positionBreakdown').textContent = 
            `${positions.LONG || 0}L / ${positions.SHORT || 0}S / ${positions.FLAT || 0}F`;
    }
    
    // Update win/loss ratio
    if (stats.median_return !== undefined) {
        const winLossEl = document.getElementById('winLossRatio');
        const winLossKey = `winloss_${coin}`;
        const oldWinLoss = previousValues[winLossKey];
        const currentWinLoss = `${stats.positive_cumulative}/${stats.negative_cumulative}`;
        
        winLossEl.innerHTML = 
            `<span style="color: var(--success)">${stats.positive_cumulative}</span>/<span style="color: var(--danger)">${stats.negative_cumulative}</span>`;
        
        if (oldWinLoss !== currentWinLoss) {
            winLossEl.classList.add('value-pulse');
            setTimeout(() => winLossEl.classList.remove('value-pulse'), 600);
        }
        
        previousValues[winLossKey] = currentWinLoss;
        
        document.getElementById('performanceDetail').innerHTML = 
            `24h: <span style="color: var(--success)">${stats.positive_24h}</span>/<span style="color: var(--danger)">${stats.negative_24h}</span> | Med: ${stats.median_return >= 0 ? '+' : ''}${stats.median_return.toFixed(2)}%`;
    }
    
    // Update performance stats
    if (stats.median_return !== undefined) {
        document.getElementById('winLossRatio').innerHTML = 
            `<span style="color: var(--success)">${stats.positive_cumulative}</span>/<span style="color: var(--danger)">${stats.negative_cumulative}</span>`;
        
        document.getElementById('performanceDetail').innerHTML = 
            `24h: <span style="color: var(--success)">${stats.positive_24h}</span>/<span style="color: var(--danger)">${stats.negative_24h}</span> | Med: ${stats.median_return >= 0 ? '+' : ''}${stats.median_return.toFixed(2)}%`;
    }
    
    // Update table title
    const tableTitle = coin === 'all' ? 'All Symbols' : `${coin.toUpperCase()} Symbols`;
    safeUpdateElement('tableTitle', 'textContent', tableTitle);
    
    // Update chart title
    const chartTitle = coin === 'all' ? 'Portfolio Performance' : `${coin.toUpperCase()} Performance`;
    safeUpdateElement('chartTitle', 'textContent', chartTitle);
}

// Show skeleton loading state
function showSkeletonLoading() {
    // Show skeleton for stats (only if element exists)
    const statsRow = document.getElementById('statsRow');
    if (!allData && statsRow) { // Only show skeleton on initial load and if element exists
        statsRow.innerHTML = '';
        for (let i = 0; i < 5; i++) {
            const skeleton = document.createElement('div');
            skeleton.className = 'skeleton-stat-card';
            skeleton.innerHTML = `
                <div class="skeleton skeleton-text" style="width: 60%;"></div>
                <div class="skeleton skeleton-text" style="width: 80%; height: 24px; margin-top: 12px;"></div>
                <div class="skeleton skeleton-text" style="width: 40%;"></div>
            `;
            statsRow.appendChild(skeleton);
        }
        
        // Show skeleton for table
        const tableBody = document.getElementById('tableBody');
        tableBody.innerHTML = '';
        for (let i = 0; i < 8; i++) {
            const row = document.createElement('div');
            row.className = 'skeleton-table-row';
            for (let j = 0; j < 10; j++) {
                const cell = document.createElement('div');
                cell.className = 'skeleton';
                row.appendChild(cell);
            }
            tableBody.appendChild(row);
        }
    }
}

// Load data from API with state preservation
async function loadData(preserveState = true, skipAnimations = false) {
    try {
        // Show skeleton loading on initial load
        if (!allData) {
            showSkeletonLoading();
        }
        // Save current state
        let savedState = null;
        if (preserveState) {
            const searchInput = document.getElementById('searchInput');
            savedState = {
                searchTerm: searchInput ? searchInput.value : '',
                scrollPosition: window.scrollY,
                currentCoin: currentCoin,
                currentView: currentView,
                currentTimeRange: currentTimeRange
            };
        }
        
        const response = await fetch('/api/symbols/summary');
        const data = await response.json();
        
        if (data.error) {
            console.error('API Error:', data.error);
            return;
        }
        
        // Only update if data has actually changed
        const dataChanged = JSON.stringify(allData?.symbols) !== JSON.stringify(data.symbols);
        
        if (dataChanged || !allData) {
            allData = data;
            symbolsData = data.symbols;
            
            // Rebuild stats row if it was skeleton
            const statsRow = document.getElementById('statsRow');
            if (statsRow && statsRow.querySelector('.skeleton-stat-card')) {
                statsRow.innerHTML = `
                    <div class="stat-card">
                        <div class="stat-header">
                            <div class="stat-icon">
                                <i class="ri-pulse-line"></i>
                            </div>
                            <div>
                                <div class="stat-label">Tests Fresh</div>
                                <div class="stat-value" id="freshnessStat">-/-</div>
                                <div class="stat-change positive" id="freshnessChange">
                                    <span id="freshnessPercent">-%</span>
                                </div>
                            </div>
                        </div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-header">
                            <div class="stat-icon">
                                <i class="ri-refresh-line"></i>
                            </div>
                            <div>
                                <div class="stat-label">Sync Status</div>
                                <div class="stat-value" id="syncStatus">-</div>
                                <div class="stat-change" id="nextSync">-</div>
                            </div>
                        </div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-header">
                            <div class="stat-icon">
                                <i class="ri-funds-line"></i>
                            </div>
                            <div>
                                <div class="stat-label">Average Return</div>
                                <div class="stat-value" id="avgReturn">-%</div>
                                <div class="stat-change" id="returnRange">-</div>
                            </div>
                        </div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-header">
                            <div class="stat-icon">
                                <i class="ri-exchange-line"></i>
                            </div>
                            <div>
                                <div class="stat-label">Positions</div>
                                <div class="stat-value" id="activePositions">-/-</div>
                                <div class="stat-change" id="positionBreakdown">-</div>
                            </div>
                        </div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-header">
                            <div class="stat-icon">
                                <i class="ri-trophy-line"></i>
                            </div>
                            <div>
                                <div class="stat-label">Win/Loss</div>
                                <div class="stat-value" id="winLossRatio">-/-</div>
                                <div class="stat-change" id="performanceDetail">-</div>
                            </div>
                        </div>
                    </div>
                `;
            }
            
            // Update UI with animations
            updateStatistics(currentCoin);
            updateTable(symbolsData, skipAnimations);
        } else if (dataChanged) {
            // Data changed but not initial load - still update
            updateStatistics(currentCoin);
            updateTable(symbolsData, skipAnimations);
        }
        
        // Restore state
        if (preserveState && savedState) {
            document.getElementById('searchInput').value = savedState.searchTerm;
            window.scrollTo(0, savedState.scrollPosition);
        }
        
        // Load chart data if needed
        if (!portfolioChartData) {
            await loadPortfolioChart();
        }
        
        // Check sync status
        checkSyncStatus();
        
    } catch (error) {
        console.error('Error loading data:', error);
    }
}

// Check sync status
async function checkSyncStatus() {
    try {
        const response = await fetch('/api/sync/status');
        const data = await response.json();
        
        const statusEl = document.getElementById('syncStatus');
        const nextSyncEl = document.getElementById('nextSync');
        
        if (data.sync_in_progress) {
            statusEl.innerHTML = '<span style="color: var(--secondary);">Syncing...</span>';
            nextSyncEl.textContent = 'In progress';
            stopSyncCountdown();
        } else if (data.thread_running || data.last_sync) {
            statusEl.innerHTML = '<span style="color: var(--success);">Active</span>';
            
            if (data.last_sync) {
                lastSyncTime = new Date(data.last_sync);
                startSyncCountdown();
            }
        } else {
            statusEl.innerHTML = '<span style="color: var(--warning);">Inactive</span>';
            nextSyncEl.textContent = 'Not scheduled';
            stopSyncCountdown();
        }
    } catch (error) {
        console.error('Error checking sync status:', error);
    }
}

// Start sync countdown
function startSyncCountdown() {
    stopSyncCountdown();
    
    updateSyncCountdown();
    syncCountdownInterval = setInterval(updateSyncCountdown, 1000);
}

// Stop sync countdown
function stopSyncCountdown() {
    if (syncCountdownInterval) {
        clearInterval(syncCountdownInterval);
        syncCountdownInterval = null;
    }
}

// Update sync countdown
function updateSyncCountdown() {
    if (!lastSyncTime) return;
    
    const now = new Date();
    const timeSinceSync = now - lastSyncTime;
    const timeUntilSync = Math.max(0, (5 * 60 * 1000) - timeSinceSync);
    
    if (timeUntilSync > 0) {
        const minutes = Math.floor(timeUntilSync / 60000);
        const seconds = Math.floor((timeUntilSync % 60000) / 1000);
        document.getElementById('nextSync').textContent = 
            `Next in ${minutes}m ${seconds}s`;
    } else {
        document.getElementById('nextSync').textContent = 'Due now';
    }
}

// Load portfolio chart
async function loadPortfolioChart() {
    try {
        const response = await fetch('/api/cumulative_returns/all');
        const data = await response.json();
        
        if (data.error) {
            console.error('Chart API Error:', data.error);
            return;
        }
        
        // Store all data, don't filter yet
        portfolioChartData = data.symbols;
        // Always render with current filter
        renderPortfolioChart();
        
    } catch (error) {
        console.error('Error loading chart:', error);
    }
}

// Render portfolio chart
function renderPortfolioChart() {
    if (!portfolioChartData || !chartVisible) return;
    
    let filteredData = portfolioChartData;
    
    // Filter by coin if selected
    if (currentCoin !== 'all') {
        filteredData = portfolioChartData.filter(s => s.symbol === currentCoin.toUpperCase());
    }
    
    if (filteredData.length === 0) {
        const isDark = document.documentElement.getAttribute('data-theme') !== 'light';
        const layout = {
            title: '',
            xaxis: { visible: false },
            yaxis: { visible: false },
            paper_bgcolor: 'transparent',
            plot_bgcolor: 'transparent',
            annotations: [{
                text: `No ${currentCoin.toUpperCase()} data available`,
                xref: 'paper',
                yref: 'paper',
                x: 0.5,
                y: 0.5,
                showarrow: false,
                font: { size: 16, color: isDark ? '#94a3b8' : '#64748b' }
            }]
        };
        Plotly.newPlot('portfolioChart', [], layout, { responsive: true, displayModeBar: false });
        return;
    }
    
    const traces = filteredData.map(symbolData => {
        let x = symbolData.data.x;
        let y = symbolData.data.y;
        
        // Apply time range filter
        if (currentTimeRange !== 'ALL') {
            const filtered = filterByTimeRange(x, y, currentTimeRange);
            x = filtered.x;
            y = filtered.y;
        }
        
        // For log scale, convert percentages to growth multipliers
        // 0% = 1.0x, 100% = 2.0x, -50% = 0.5x
        let displayY = y;
        let hoverTemplate = '%{fullData.name}<br>%{x}<br>Return: %{y:.2f}%<extra></extra>';
        
        if (useLogScale) {
            displayY = y.map(val => (100 + val) / 100);  // Convert to multiplier
            hoverTemplate = '%{fullData.name}<br>%{x}<br>Return: %{customdata:.2f}%<extra></extra>';
        }
        
        // Format bucket name (e.g., "cygnus1_data_live_tradingpp1" -> "pp1")
        const bucketShort = symbolData.bucket ? symbolData.bucket.replace(/.*trading/, '').toUpperCase() : '';
        
        return {
            x: x,
            y: displayY,
            customdata: y,  // Store original percentages for hover
            name: `${symbolData.symbol} TS-${symbolData.ts_id} ${bucketShort}`,
            type: 'scatter',
            mode: 'lines',
            line: {
                color: symbolData.color,
                width: 2
            },
            hovertemplate: hoverTemplate
        };
    });
    
    const isDark = document.documentElement.getAttribute('data-theme') !== 'light';
    const layout = {
        title: '',
        xaxis: {
            title: '',
            gridcolor: isDark ? 'rgba(148, 163, 184, 0.05)' : 'rgba(0, 0, 0, 0.05)',
            zerolinecolor: isDark ? 'rgba(148, 163, 184, 0.1)' : 'rgba(0, 0, 0, 0.1)',
            color: isDark ? '#64748b' : '#475569'
        },
        yaxis: {
            title: useLogScale ? 'Cumulative Return (%, Log Scale)' : 'Cumulative Return (%)',
            gridcolor: isDark ? 'rgba(148, 163, 184, 0.05)' : 'rgba(0, 0, 0, 0.05)',
            zerolinecolor: isDark ? 'rgba(148, 163, 184, 0.1)' : 'rgba(0, 0, 0, 0.1)',
            color: isDark ? '#64748b' : '#475569',
            type: useLogScale ? 'log' : 'linear',
            tickformat: useLogScale ? '' : '.1f',
            tickvals: useLogScale ? [0.5, 0.75, 1, 1.5, 2, 3, 5, 10] : undefined,
            ticktext: useLogScale ? ['-50%', '-25%', '0%', '50%', '100%', '200%', '400%', '900%'] : undefined
        },
        paper_bgcolor: 'transparent',
        plot_bgcolor: 'transparent',
        legend: {
            x: 0,
            y: 1,
            bgcolor: isDark ? 'rgba(15, 23, 42, 0.8)' : 'rgba(255, 255, 255, 0.9)',
            font: { color: isDark ? '#94a3b8' : '#475569', size: 11 }
        },
        margin: { l: 60, r: 30, t: 30, b: 40 },
        hovermode: 'x unified'
    };
    
    const config = {
        responsive: true,
        displayModeBar: false
    };
    
    Plotly.newPlot('portfolioChart', traces, layout, config);
}

// Filter data by time range
function filterByTimeRange(x, y, range) {
    if (!x || !y || x.length === 0) return { x: [], y: [] };
    
    const now = new Date();
    let cutoffDate;
    
    switch(range) {
        case '1D':
            cutoffDate = new Date(now.getTime() - 24 * 60 * 60 * 1000);
            break;
        case '1W':
            cutoffDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
            break;
        case '1M':
            cutoffDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
            break;
        case '3M':
            cutoffDate = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000);
            break;
        default:
            return { x, y };
    }
    
    const filteredIndices = [];
    x.forEach((dateStr, i) => {
        const date = new Date(dateStr);
        if (date >= cutoffDate) {
            filteredIndices.push(i);
        }
    });
    
    return {
        x: filteredIndices.map(i => x[i]),
        y: filteredIndices.map(i => y[i])
    };
}

// Update table
function updateTable(data, skipAnimations = false) {
    const tbody = document.getElementById('tableBody');
    if (!tbody) return; // Exit if table doesn't exist
    tbody.innerHTML = '';
    
    // Filter by current coin if selected
    let filteredData = [...data]; // Create a copy to avoid mutating original
    if (currentCoin !== 'all') {
        filteredData = filteredData.filter(s => s.symbol === currentCoin.toUpperCase());
    }
    
    // Apply search filter
    if (searchTerm) {
        filteredData = filteredData.filter(s => 
            s.symbol.toLowerCase().includes(searchTerm.toLowerCase()) ||
            s.trading_pair.toLowerCase().includes(searchTerm.toLowerCase()) ||
            s.position.toLowerCase().includes(searchTerm.toLowerCase()) ||
            s.bucket.toLowerCase().includes(searchTerm.toLowerCase())
        );
    }
    
    // Apply sorting - default to symbol + bucket if no sort selected
    const columnToSort = sortColumn || 'symbol';
    const directionToUse = sortColumn ? sortDirection : 'asc';
    
    filteredData.sort((a, b) => {
        let aVal, bVal;
        
        // If no explicit sort, sort by symbol then bucket for consistent order
        if (!sortColumn) {
            // First sort by symbol
            const symbolCompare = a.symbol.localeCompare(b.symbol);
            if (symbolCompare !== 0) return symbolCompare;
            // Then by bucket if symbols are the same
            return (a.bucket_raw || '').localeCompare(b.bucket_raw || '');
        }
        
        switch(columnToSort) {
                case 'symbol':
                    aVal = a.symbol;
                    bVal = b.symbol;
                    break;
                case 'bucket':
                    aVal = a.bucket_raw || '';
                    bVal = b.bucket_raw || '';
                    break;
                case 'pair':
                    aVal = a.trading_pair;
                    bVal = b.trading_pair;
                    break;
                case 'freshness':
                    aVal = a.minutes_ago;
                    bVal = b.minutes_ago;
                    break;
                case 'price':
                    aVal = a.last_price;
                    bVal = b.last_price;
                    break;
                case 'position':
                    aVal = a.position;
                    bVal = b.position;
                    break;
                case 'return':
                    aVal = a.cumulative_return;
                    bVal = b.cumulative_return;
                    break;
                case '24h':
                    aVal = a.change_24h;
                    bVal = b.change_24h;
                    break;
                case 'max':
                    aVal = a.max_return || 0;
                    bVal = b.max_return || 0;
                    break;
                default:
                    return 0;
            }
            
            // Handle string vs number comparison
            if (typeof aVal === 'string' && typeof bVal === 'string') {
                return directionToUse === 'asc' ? 
                    aVal.localeCompare(bVal) : 
                    bVal.localeCompare(aVal);
            } else {
                if (directionToUse === 'asc') {
                    return aVal > bVal ? 1 : aVal < bVal ? -1 : 0;
                } else {
                    return aVal < bVal ? 1 : aVal > bVal ? -1 : 0;
                }
            }
        });
    
    // Update clear filters button visibility
    const clearBtn = document.getElementById('clearFiltersBtn');
    if (searchTerm || currentCoin !== 'all' || sortColumn) {
        clearBtn.classList.add('visible');
    } else {
        clearBtn.classList.remove('visible');
    }
    
    filteredData.forEach((symbol, index) => {
        const row = document.createElement('tr');
        if (!skipAnimations) {
            row.style.animationDelay = `${Math.min(index * 0.02, 0.5)}s`;
        } else {
            row.classList.add('no-animation');
        }
        row.onclick = () => viewDetail(symbol.bucket_raw, symbol.ts_id);
        
        // Get symbol initial for icon
        const initial = symbol.symbol.charAt(0);
        
        // Format bucket name - extract pp1/pp2/pp3
        const bucketName = symbol.bucket_raw ? 
            symbol.bucket_raw.replace('cygnus1_data_live_tradingpp', 'pp') : '';
        
        row.innerHTML = `
            <td>
                <div class="symbol-cell">
                    <div class="symbol-icon">${initial}</div>
                    <div>
                        <div style="font-weight: 600;">${symbol.symbol}</div>
                        <div style="font-size: 0.75rem; color: var(--text-secondary);">TS-${symbol.ts_id}</div>
                    </div>
                </div>
            </td>
            <td style="font-family: 'JetBrains Mono', monospace; color: var(--text-secondary); font-size: 0.8rem;">${bucketName}</td>
            <td>${symbol.trading_pair}</td>
            <td>${formatFreshness(symbol.freshness, symbol.minutes_ago)}</td>
            <td style="font-family: 'JetBrains Mono', monospace;">$${symbol.last_price.toLocaleString()}</td>
            <td>${formatPosition(symbol.position)}</td>
            <td>${formatReturn(symbol.cumulative_return)}</td>
            <td>${formatReturn(symbol.change_24h)}</td>
            <td>${formatReturn(symbol.max_return || 0)}</td>
            <td>
                <button class="action-btn" onclick="event.stopPropagation(); viewDetail('${symbol.bucket_raw}', '${symbol.ts_id}')">
                    View
                </button>
            </td>
        `;
        
        tbody.appendChild(row);
    });
}

// Show top performers
function showTopPerformers() {
    const grid = document.getElementById('performersGrid');
    grid.innerHTML = '';
    
    // Filter data by current coin
    let filteredSymbols = symbolsData;
    if (currentCoin !== 'all') {
        filteredSymbols = symbolsData.filter(s => s.symbol === currentCoin.toUpperCase());
    }
    
    if (filteredSymbols.length === 0) {
        grid.innerHTML = `
            <div style="grid-column: 1/-1; text-align: center; padding: 3rem; color: var(--text-secondary);">
                No ${currentCoin.toUpperCase()} data available
            </div>
        `;
        grid.style.display = 'grid';
        return;
    }
    
    // Sort data for different categories
    const sortedByCumulative = [...filteredSymbols].sort((a, b) => b.cumulative_return - a.cumulative_return);
    const sortedBy24h = [...filteredSymbols].sort((a, b) => b.change_24h - a.change_24h);
    const sortedBy7d = [...filteredSymbols].sort((a, b) => (b.change_7d || 0) - (a.change_7d || 0));
    const withStreak = [...filteredSymbols]
        .filter(s => s.consecutive_positive_days > 0)
        .sort((a, b) => b.consecutive_positive_days - a.consecutive_positive_days);
    const sortedByVolatility = [...filteredSymbols].sort((a, b) => b.max_return - a.max_return);
    
    // Create performer cards
    const categories = [
        { title: '🚀 Top Gainers', subtitle: 'Cumulative Return', data: sortedByCumulative.slice(0, 5), metric: 'cumulative_return' },
        { title: '⏰ 24-Hour Stars', subtitle: '24h Change', data: sortedBy24h.slice(0, 5), metric: 'change_24h' },
        { title: '🔥 Hot Streak', subtitle: 'Positive Days', data: withStreak.slice(0, 5), metric: 'consecutive_positive_days' },
        { title: '📈 Trending Up', subtitle: '7-Day Change', data: sortedBy7d.slice(0, 5), metric: 'change_7d' },
        { title: '📊 Most Volatile', subtitle: 'Max Return', data: sortedByVolatility.slice(0, 5), metric: 'max_return' },
        { title: '📉 Bottom Performers', subtitle: 'Cumulative Return', data: sortedByCumulative.slice(-5).reverse(), metric: 'cumulative_return' }
    ];
    
    categories.forEach((category, catIndex) => {
        const card = document.createElement('div');
        card.className = 'performer-card';
        card.style.animationDelay = `${catIndex * 0.1}s`;
        
        let itemsHtml = '';
        category.data.forEach((symbol, index) => {
            let metricDisplay;
            
            if (category.metric === 'consecutive_positive_days') {
                const days = symbol.consecutive_positive_days || 0;
                const fireCount = Math.min(Math.floor(days / 2) + 1, 3);
                metricDisplay = '🔥'.repeat(fireCount) + ` ${days}d`;
            } else {
                const value = symbol[category.metric] || 0;
                metricDisplay = formatReturn(value);
            }
            
            // Format bucket name for display - just show pp1/pp2/pp3
            const bucketDisplay = symbol.bucket_raw ? symbol.bucket_raw.replace('cygnus1_data_live_tradingpp', 'pp') : '';
            
            itemsHtml += `
                <div class="performer-item" onclick="viewDetail('${symbol.bucket_raw}', '${symbol.ts_id}')">
                    <div style="display: flex; align-items: center; gap: 0.5rem; flex: 1;">
                        <span style="color: var(--text-secondary); font-size: 0.75rem; min-width: 20px;">#${index + 1}</span>
                        <div style="flex: 1;">
                            <div style="font-weight: 600;">${symbol.symbol} <span style="font-weight: 400; color: var(--text-secondary); font-size: 0.85rem;">(${symbol.trading_pair})</span></div>
                            <div style="font-size: 0.65rem; color: var(--text-secondary); opacity: 0.8;">TS-${symbol.ts_id} • ${bucketDisplay}</div>
                        </div>
                    </div>
                    <div style="text-align: right;">${metricDisplay}</div>
                </div>
            `;
        });
        
        card.innerHTML = `
            <div class="performer-header">
                <div class="performer-title">${category.title}</div>
                <div class="performer-subtitle">${category.subtitle}</div>
            </div>
            ${itemsHtml || '<div style="text-align: center; color: var(--text-secondary); padding: 1rem;">No data</div>'}
        `;
        
        grid.appendChild(card);
    });
    
    grid.style.display = 'grid';
}

// View symbol detail
function viewDetail(bucket, tsId) {
    window.location.href = `/symbol/${bucket}/TS-${tsId}`;
}

// Show/hide loading with random messages
function showLoading() {
    const messages = [
        'Fetching latest market data...',
        'Analyzing trading positions...',
        'Calculating returns...',
        'Syncing with exchange...',
        'Loading portfolio data...'
    ];
    const randomMessage = messages[Math.floor(Math.random() * messages.length)];
    document.querySelector('.loading-subtext').textContent = randomMessage;
    document.getElementById('loadingOverlay').classList.add('active');
}

function hideLoading() {
    setTimeout(() => {
        document.getElementById('loadingOverlay').classList.remove('active');
    }, 500); // Small delay for smooth transition
}

// Clear all filters
function clearAllFilters() {
    searchTerm = '';
    document.getElementById('searchInput').value = '';
    currentCoin = 'all';
    sortColumn = null;
    sortDirection = 'asc';
    
    // Clear sort indicators
    document.querySelectorAll('.sortable').forEach(th => {
        th.classList.remove('sort-asc', 'sort-desc');
    });
    
    // Update navigation - only change the view button, keep coin filter
    document.querySelectorAll('.nav-pill:not(.coin-btn)').forEach(p => {
        p.classList.remove('active');
    });
    document.getElementById('overviewBtn').classList.add('active');
    
    // Update view
    currentView = 'overview';
    document.getElementById('performersGrid').style.display = 'none';
    document.getElementById('chartContainer').style.display = 'block';
    document.getElementById('tableContainer').style.display = 'block';
    
    // Update UI - respect current coin filter
    updateStatistics(currentCoin);
    
    // Filter table data if coin is selected
    if (currentCoin !== 'all') {
        const filteredData = symbolsData.filter(s => s.symbol === currentCoin.toUpperCase());
        updateTable(filteredData);
    } else {
        updateTable(symbolsData);
    }
    
    renderPortfolioChart();
}

// Initialize event listeners
function initEventListeners() {
    // Navigation pills
    document.querySelectorAll('.nav-pill').forEach(pill => {
        pill.addEventListener('click', (e) => {
            const view = e.currentTarget.dataset.view;
            const coin = e.currentTarget.dataset.coin;
            
            if (view) {
                // View navigation
                document.querySelectorAll('.nav-pill[data-view]').forEach(p => p.classList.remove('active'));
                e.currentTarget.classList.add('active');
                currentView = view;
                
                if (view === 'performers') {
                    document.getElementById('chartContainer').style.display = 'none';
                    document.getElementById('tableContainer').style.display = 'none';
                    showTopPerformers();
                } else {
                    document.getElementById('performersGrid').style.display = 'none';
                    document.getElementById('chartContainer').style.display = 'block';
                    document.getElementById('tableContainer').style.display = 'block';
                    
                    // Update the view with current filters
                    updateStatistics(currentCoin);
                    
                    // Filter table if coin is selected
                    if (currentCoin !== 'all') {
                        const filteredData = symbolsData.filter(s => s.symbol === currentCoin.toUpperCase());
                        updateTable(filteredData);
                    } else {
                        updateTable(symbolsData);
                    }
                    
                    renderPortfolioChart();
                }
            } else if (coin) {
                // Coin filter
                document.querySelectorAll('.nav-pill[data-coin]').forEach(p => p.classList.remove('active-coin'));
                e.currentTarget.classList.add('active-coin');
                currentCoin = coin;
                
                // Update everything based on coin filter
                updateStatistics(currentCoin);
                
                // Filter table if coin is selected
                if (currentCoin !== 'all') {
                    const filteredData = symbolsData.filter(s => s.symbol === currentCoin.toUpperCase());
                    updateTable(filteredData);
                } else {
                    updateTable(symbolsData);
                }
                
                if (currentView === 'performers') {
                    showTopPerformers();
                } else {
                    renderPortfolioChart();
                }
            }
        });
    });
    
    // Time range buttons
    document.querySelectorAll('.time-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            document.querySelectorAll('.time-btn').forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');
            currentTimeRange = e.target.dataset.range;
            renderPortfolioChart();
        });
    });
    
    // Search input (only if it exists)
    const searchInput = document.getElementById('searchInput');
    if (searchInput) {
        searchInput.addEventListener('input', (e) => {
            searchTerm = e.target.value;
            updateTable(symbolsData);
        });
    }
    
    // Clear filters button (only if it exists)
    const clearFiltersBtn = document.getElementById('clearFiltersBtn');
    if (clearFiltersBtn) {
        clearFiltersBtn.addEventListener('click', clearAllFilters);
    }
    
    // Sortable column headers
    document.querySelectorAll('.sortable').forEach(th => {
        th.addEventListener('click', (e) => {
            const column = e.currentTarget.dataset.column;
            
            // Update sort state
            if (sortColumn === column) {
                // Toggle direction if same column
                sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
            } else {
                // New column, default to descending for numeric columns
                sortColumn = column;
                sortDirection = ['return', '24h', '7d', 'price'].includes(column) ? 'desc' : 'asc';
            }
            
            // Update visual indicators
            document.querySelectorAll('.sortable').forEach(header => {
                header.classList.remove('sort-asc', 'sort-desc');
            });
            e.currentTarget.classList.add(sortDirection === 'asc' ? 'sort-asc' : 'sort-desc');
            
            // Update table
            updateTable(symbolsData);
        });
    });
    
    // Refresh button (only if it exists)
    const refreshBtn = document.getElementById('refreshBtn');
    if (refreshBtn) {
        refreshBtn.addEventListener('click', () => {
            showLoading();
            loadData(true).then(() => hideLoading());
        });
    }
    
    // Chart toggle (only if it exists)
    const chartToggle = document.getElementById('chartToggle');
    if (chartToggle) {
        chartToggle.addEventListener('click', () => {
            const chart = document.getElementById('portfolioChart');
            chartVisible = !chartVisible;
            if (chartVisible) {
                chart.style.display = 'block';
                renderPortfolioChart();
                chartToggle.innerHTML = '<i class="ri-eye-line"></i>';
            } else {
                chart.style.display = 'none';
                chartToggle.innerHTML = '<i class="ri-eye-off-line"></i>';
            }
        });
    }
    
    // Scale toggle (only if it exists)
    const scaleToggle = document.getElementById('scaleToggle');
    if (scaleToggle) {
        scaleToggle.addEventListener('click', () => {
        useLogScale = !useLogScale;
        const scaleBtn = document.getElementById('scaleToggle');
        const scaleLabel = scaleBtn.querySelector('.scale-label');
        
        if (useLogScale) {
            scaleBtn.classList.add('log-scale');
            scaleLabel.textContent = 'Log';
        } else {
            scaleBtn.classList.remove('log-scale');
            scaleLabel.textContent = 'Linear';
        }
        
        // Re-render the chart with new scale
            if (chartVisible && portfolioChartData) {
                renderPortfolioChart();
            }
        });
    }
}

// Auto refresh
function startAutoRefresh() {
    autoRefreshInterval = setInterval(() => {
        loadData(true, true); // preserveState = true, skipAnimations = true
    }, 30000); // 30 seconds
}

function stopAutoRefresh() {
    if (autoRefreshInterval) {
        clearInterval(autoRefreshInterval);
    }
}

// Create subtle floating particles
function createParticles() {
    const container = document.getElementById('particlesContainer');
    const particleCount = 30;
    
    for (let i = 0; i < particleCount; i++) {
        const particle = document.createElement('div');
        particle.className = 'particle';
        particle.style.left = Math.random() * 100 + '%';
        particle.style.animationName = 'particleFloat';
        particle.style.animationDuration = (20 + Math.random() * 20) + 's';
        particle.style.animationDelay = Math.random() * 20 + 's';
        particle.style.animationIterationCount = 'infinite';
        container.appendChild(particle);
    }
}

// Theme management
let currentTheme = 'dark';
function initTheme() {
    // Load saved preferences
    currentTheme = localStorage.getItem('theme') || 'dark';
    currentAccent = localStorage.getItem('accent') || 'default';
    
    // Apply theme
    document.documentElement.setAttribute('data-theme', currentTheme);
    
    // Apply accent
    if (currentAccent !== 'default') {
        document.documentElement.setAttribute('data-accent', currentAccent);
    }
    
    // Set default animation speed
    document.documentElement.style.setProperty('--animation-speed', '1');
    
    // Update UI
    document.querySelectorAll('.color-option').forEach(btn => {
        if (btn.dataset.accent === currentAccent) {
            btn.classList.add('active');
        }
    });
}

function initThemeEventListeners() {
    // Theme toggle button
    document.getElementById('themeToggleBtn').addEventListener('click', () => {
        const switcher = document.getElementById('themeSwitcher');
        switcher.classList.toggle('expanded');
    });
    
    // Light/Dark mode buttons
    const lightModeBtn = document.getElementById('lightModeBtn');
    const darkModeBtn = document.getElementById('darkModeBtn');
    
    // Update initial button states
    updateModeButtons();
    
    if (lightModeBtn && darkModeBtn) {
        lightModeBtn.addEventListener('click', () => {
            currentTheme = 'light';
            document.documentElement.setAttribute('data-theme', 'light');
            localStorage.setItem('theme', 'light');
            updateModeButtons();
            updateChartTheme();
        });
        
        darkModeBtn.addEventListener('click', () => {
            currentTheme = 'dark';
            document.documentElement.setAttribute('data-theme', 'dark');
            localStorage.setItem('theme', 'dark');
            updateModeButtons();
            updateChartTheme();
        });
    }
    
    // Color options
    document.querySelectorAll('.color-option').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const accent = e.currentTarget.dataset.accent;
            currentAccent = accent;
            localStorage.setItem('accent', accent);
            
            document.querySelectorAll('.color-option').forEach(b => b.classList.remove('active'));
            e.currentTarget.classList.add('active');
            
            if (accent === 'default') {
                document.documentElement.removeAttribute('data-accent');
            } else {
                document.documentElement.setAttribute('data-accent', accent);
            }
        });
    });
    
    // Close theme switcher when clicking outside
    document.addEventListener('click', (e) => {
        const switcher = document.getElementById('themeSwitcher');
        if (!switcher.contains(e.target)) {
            switcher.classList.remove('expanded');
        }
    });
}

// Helper functions for theme
function updateModeButtons() {
    const lightBtn = document.getElementById('lightModeBtn');
    const darkBtn = document.getElementById('darkModeBtn');
    
    if (currentTheme === 'light') {
        lightBtn?.classList.add('active');
        darkBtn?.classList.remove('active');
    } else {
        darkBtn?.classList.add('active');
        lightBtn?.classList.remove('active');
    }
}

function updateChartTheme() {
    const isDark = currentTheme === 'dark';
    const layout = {
        paper_bgcolor: 'transparent',
        plot_bgcolor: 'transparent',
        font: { color: isDark ? '#f1f5f9' : '#0f172a' },
        xaxis: { 
            gridcolor: isDark ? 'rgba(148, 163, 184, 0.1)' : 'rgba(0, 0, 0, 0.08)',
            zerolinecolor: isDark ? 'rgba(148, 163, 184, 0.1)' : 'rgba(0, 0, 0, 0.1)',
            tickfont: { color: isDark ? '#94a3b8' : '#475569' },
            titlefont: { color: isDark ? '#f1f5f9' : '#0f172a' }
        },
        yaxis: { 
            gridcolor: isDark ? 'rgba(148, 163, 184, 0.1)' : 'rgba(0, 0, 0, 0.08)',
            zerolinecolor: isDark ? 'rgba(148, 163, 184, 0.1)' : 'rgba(0, 0, 0, 0.1)',
            tickfont: { color: isDark ? '#94a3b8' : '#475569' },
            titlefont: { color: isDark ? '#f1f5f9' : '#0f172a' }
        },
        legend: {
            x: 0,
            y: 1,
            bgcolor: isDark ? 'rgba(15, 23, 42, 0.8)' : 'rgba(255, 255, 255, 0.9)',
            font: { color: isDark ? '#94a3b8' : '#475569', size: 11 }
        }
    };
    
    // Update portfolio chart if it exists
    const chartDiv = document.getElementById('portfolioChart');
    if (chartDiv && chartDiv.data) {
        Plotly.relayout('portfolioChart', layout);
    }
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', async () => {
    initTheme();
    initThemeEventListeners();
    createParticles();
    initEventListeners();
    showLoading();
    await loadData(false);
    hideLoading();
    startAutoRefresh();
    
    // Set initial active states (only if element exists)
    const allBtn = document.getElementById('allBtn');
    if (allBtn) {
        allBtn.classList.add('active-coin');
    }
});

// Handle visibility change
document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
        stopAutoRefresh();
        stopSyncCountdown();
    } else {
        loadData(true, true); // Skip animations when coming back to the page
        startAutoRefresh();
    }
});