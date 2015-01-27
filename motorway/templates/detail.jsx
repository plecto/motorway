var Detail = React.createClass({

	getInitialState: function() {
		return {
			'failedMessages': []
		}
	},

	componentDidMount: function() {
		this.loadState();
	},

	render: function() {
		return (
			<div>
				{this.state.failedMessages.map(function(msg) {
					return (<pre className="failed-message">{msg}</pre>)
				})}
			</div>
		)
	},

	loadState: function() {
		var that = this;
		$.getJSON(DetailSettings.apiUrl, function(data) {
			if (that.isMounted()) {
				that.setState({
					'failedMessages': data.failed_messages
				});
			}
		})
		.done(function() {
			$('body').removeClass('disconnected');
		})
		.fail(function() {
			$('body').addClass('disconnected');
		})
		.always(function() {
			var pulse = $('#pulse');
			pulse.show();
			setTimeout(function() {
				pulse.hide();
			}, 100);
			setTimeout(function() {
				that.loadState();
			}, 10000);
		});
	}
});

React.render(<Detail />, document.querySelector('#container'));

