var GroupContainer = require('./components/GroupContainer.jsx');
var Utils = require('./utils.js');

var Pipeline = React.createClass({

	getInitialState: function() {
		return {
			'groups': []
		}
	},

	componentDidMount: function() {
		this.loadState();
	},

	render: function() {
		if (Object.keys(this.state.groups).length) {
			return (
				<div className="pipeline">
					<GroupContainer groups={this.state.groups} lastMinutes={this.state.lastMinutes} />
				</div>
				)
		} else {
			return (
				<div className="loader">
					<span className="fa fa-circle-o-notch fa-spin"></span>
					<p>Motorway is loading...</p>
				</div>
			)
		}
	},

	getNodeData: function(node) {
		var nodeData = {
			'name': node[0],
			'histogram': node[1]['histogram'],
			'avgTime': Utils.getISODuration(node[1]['avg_time_taken']),
			'percentile': Utils.getISODuration(node[1]['95_percentile']),
			'waiting': node[1]['waiting']
		};
		var nodeTitle = nodeData.name.replace(/([A-Z])/g, ' $1');
		nodeTitle = nodeTitle.split('-')[0].split(' ');
		var nodeType = nodeTitle.splice(nodeTitle.length-1, 1).join(' ').toLowerCase();
		nodeTitle = nodeTitle.join(' ');
		nodeData.title = nodeTitle;
		nodeData.nodeType = nodeType;
		nodeData.status = node[1]['status'];

		var icons = {
			'ramp': 'road',
			'intersection': 'exchange',
			'tap': 'road',
			'transformer': 'exchange'
		};
		nodeData.iconClass = 'fa fa-'+icons[nodeType];

		nodeData.secondsRemaining = Utils.getSecondsFromDuration(nodeData.avgTime) * nodeData.waiting;

		nodeData.url = PipelineSettings.detailUrlPrefix + nodeData.name;

		return nodeData;
	},

	loadState: function() {
		var that = this;
		var pipelineURL;
		if (__DEV__) {
			pipelineURL = 'http://localhost:5000/api/status/';
		} else {
			pipelineURL = '/api/status/';
		}
		$.getJSON(pipelineURL, function(data) {
			if (that.isMounted()) {
				that.setState({
					'groups': data['groups'],
					//'maxHistogramValue': maxHistogramValue,
					'lastMinutes': data['last_minutes']
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
			}, 1000);
		});
	}
});

ReactDOM.render(<Pipeline />, document.querySelector('#container'));

