import React, { Component } from 'react'; //different
import logo from './logo.svg';
import './App.css';
import StockInfo from './StockInfo';
class App extends Component {
  //different
  constructor(props) {
    super(props);

    this.state = {
      results: [],
    };
    this.fetchProfileStockInfo = this.fetchProfileStockInfo.bind(this);
  }

  componentDidMount() {
    this.fetchProfileStockInfo();
  }
  
  fetchProfileStockInfo() {
    var me = this;
    fetch('http://localhost:5091')
      .then(
        function (response) {
          if (response.status !== 200) {
            console.log('Looks like there was a problem. Status Code: ' +
              response.status);
            return;
          }

          // Examine the text in the response
          response.json().then(function (data) {
            //console.log(data);
            me.setState({ results: data })
            //console.log(me.state.results)
          })
        }
      )
      .catch(function (err) {
        console.log('Fetch Error :-S', err);
      });

  }
  
  render() { //different
    // The rest of the file is the same
    //console.log(this.state.results)
    //this.fetchProfileStockInfo();
    return (
      <div>
        <p>Profile</p>
        <meta httpEquiv="refresh" content="300" ></meta>
        {
          this.state.results.map(result => <StockInfo data={result} />)
        }
      </div>
    
    )
  };
}

export default App;