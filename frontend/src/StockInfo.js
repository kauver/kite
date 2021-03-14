import React from 'react';
import { CartesianGrid, Line, LineChart, Tooltip, XAxis, YAxis, Legend } from 'recharts';


const StockInfo = (props) => {
    console.log('props.data', props.data);
    const {
        data,
    } = props;
    const symbol = data['Meta Data']['2. Symbol'];
    const lastRefreshed = data['Meta Data']['3. Last Refreshed'];
    const timezone = data['Meta Data']['5. Time Zone'];

    console.log('data', data);
    const timeSeries = data['Time Series (Daily)'];
    const rows = [];
    for (var key in timeSeries) {
        if (timeSeries[key]) {
            const finData = timeSeries[key];
            const open = finData['1. open'];
            const high = finData['2. high'];
            const low = finData['3. low'];
            const close = finData['4. close'];
            const volume = finData['5. volume'];

            rows.push({
                date: key,
                open,
                high,
                low,
                close,
            })
        }
    }

    console.log('rows', rows);

    return (
        <div>

            <p>{symbol}</p>
            <p>{lastRefreshed}</p>
            <p>{timezone}</p>
            <br />

            <LineChart width={600} height={300} data={rows}
                margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                <XAxis dataKey="date" />
                <YAxis />
                <CartesianGrid strokeDasharray="3 3" />
                <Tooltip />
                <Legend />
                <Line type="monotone" dataKey="open" stroke="#0000FF" dot={false} activeDot={{ r: 8 }} />
                <Line type="monotone" dataKey="high" stroke="#00FF00" dot={false} />
                <Line type="monotone" dataKey="low" stroke="#800000" dot={false} />
                <Line type="monotone" dataKey="close" stroke="#FFA500" dot={false} />
            </LineChart>
        </div>
    )
};

export default StockInfo;