var NodeGraph = React.createClass({
	render: function() {
		var histogram = this.props.histogram;

		var maxHistogramValue = 0;
		$.each(histogram, function(minute, item) {
			maxHistogramValue = Math.max(maxHistogramValue, (item['success_count'] + item['error_count'] + item['timeout_count']));
		});

		$.each(histogram, function(minute, item) {
			if (maxHistogramValue > 0) {
				histogram[minute]['success_percentage'] = (item['success_count'] / maxHistogramValue) * 100;
				histogram[minute]['error_percentage'] = (item['error_count'] / maxHistogramValue) * 100;
				histogram[minute]['timeout_percentage'] = (item['timeout_count'] / maxHistogramValue) * 100;
			} else {
				histogram[minute]['success_percentage'] = 0;
				histogram[minute]['error_percentage'] = 0;
				histogram[minute]['timeout_percentage'] = 0;
			}
		});
		var latestHistogram = this.props.lastMinutes.map(function(minute) {
			return {
				'minute': minute,
				'value': histogram[minute]
			};
		});
		return (
			<div className="node-graph">
				{latestHistogram.map(function (item) {
					var blank = {
						'height': parseInt(100-parseInt(item.value.success_percentage)-parseInt(item.value.error_percentage)-parseInt(item.value.timeout_percentage))+'%'
					};
					var success = {
						'height': parseInt(item.value.success_percentage)+'%'
					};
					var timeout = {
						'height': parseInt(item.value.timeout_percentage)+'%'
					};
					var error = {
						'height': parseInt(item.value.error_percentage)+'%'
					};
					return <span className="node-graph-bar" key={item.minute}>
						<span className="node-graph-bar-part node-graph-blank" style={blank}></span>
						<span className="node-graph-bar-part node-graph-success" style={success}>{item.value.success_count}</span>
						<span className="node-graph-bar-part node-graph-timeout" style={timeout}>{item.value.timeout_count}</span>
						<span className="node-graph-bar-part node-graph-error" style={error}>{item.value.error_count}</span>
					</span>
				})}
			</div>
		)
	}
});

module.exports = NodeGraph;