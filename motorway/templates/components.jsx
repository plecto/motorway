var NodesContainer = React.createClass({
	render: function() {
		// node size class
		var nodeSizeClass = '';
		if (this.props.nodes.length > 30) {
			nodeSizeClass = ' node-size-42';
		} else if (this.props.nodes.length > 20) {
			nodeSizeClass = ' node-size-30';
		}

		return (
			<div className="nodes-container">
			{$.map(this.props.nodes, function (node) {
				// node status class
				var nodeClass = 'node' + nodeSizeClass;
				if (node.waiting < 1) {
					nodeClass += ' node-status-zero';
				} else if (node.waiting < 10) {
					nodeClass += ' node-status-low';
				}
				if (node.secondsRemaining > PipelineSettings.remainingSecondsDanger) {
					nodeClass += ' node-status-danger';
				} else if (node.secondsRemaining > PipelineSettings.remainingSecondsWarning) {
					nodeClass += ' node-status-warning';
				}

				return (
					<div key={node.name} className={nodeClass}>
						<div className="node-inner">
							<NodeCircle size={node.secondsRemaining} />
							<div className="node-content">
								<span className="node-icon"><span className={node.iconClass}></span></span>
								<a href={node.url} target="_blank" className="node-name">{node.title}</a>
								<p className="node-type">{node.nodeType}</p>
								<div className="node-time">
									<p className="node-time-percentile">{Utils.formatISODuration(node.percentile)}</p>
									<p className="node-time-average">{Utils.formatISODuration(node.avgTime)}</p>
								</div>
								<div className="node-waiting">
									<p className="node-waiting-main">{Utils.formatSeconds(node.secondsRemaining)}</p>
									<h2 className="node-waiting-alt">{node.waiting}</h2>
								</div>
							</div>
							<NodeGraph items={node.latestHistogram} />
						</div>
					</div>
				)
			})}
			</div>
		)
	}
});

var NodeCircle = React.createClass({
	render: function() {
		var size = parseInt(this.props.size);
		var style = {
			'width': size,
			'height': size,
			'marginTop': -parseInt(size/2) - 20,
			'marginLeft': -parseInt(size/2) + 20
		};
		return (
			<div className="node-circle" style={style}></div>
		)
	}
});

var NodeGraph = React.createClass({
	render: function() {
		return (
			<div className="node-graph">
				{this.props.items.map(function (item) {
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
