var NodesContainer = React.createClass({

	getNodeSize: function(count) {
		var ratio = 0.8;
		var numHoriz = parseInt(Math.sqrt(count/ratio));
		var width = 100/numHoriz;
		var numVert = Math.ceil(count / numHoriz);
		return [width, 100 / numVert];
	},

	render: function() {
		// node size
		var nodeSize = this.getNodeSize(this.props.nodes.length);
		var nodeStyle = {
			'width': nodeSize[0]+'%',
			'height': nodeSize[1]+'%'
		};

		return (
			<div className="nodes-container">
			{$.map(this.props.nodes, function (node) {
				// node status class
				var nodeClass = 'node';
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
					<div key={node.name} className={nodeClass} style={nodeStyle}>
						<div className="node-inner">
							<NodeCircle size={node.secondsRemaining} />
							<div className="node-content">
								<span className="node-icon"><span className={node.iconClass}></span></span>
								<a href={node.url} target="_blank" className="node-name">{node.title} {node.status}2</a>
								<p className="node-type">{node.nodeType}</p>
								<div className="node-time">
									<p className="node-time-average">x&#772;: {Utils.formatISODuration(node.avgTime)}</p>
                                    <p className="node-time-percentile">95%: {Utils.formatISODuration(node.percentile)}</p>
								</div>
								<div className="node-waiting">
									<p className="node-waiting-main">{node.waiting} / {Utils.formatSeconds(node.secondsRemaining)}</p>
									<h2 className="node-waiting-alt">&#35; Waiting / Est. time to process</h2>
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

var GroupContainer = React.createClass({
	render: function() {
		that = this;
		return (
			<div className="group-container">
			{$.map(Object.keys(that.props.groups), function (groupName) {
			return (
				<div className="group">
					<h1><span className="circle-icon"><i className="fa fa-exchange"></i></span>{groupName}</h1>
					<ul className="status-icons">
					{$.map(Object.keys(that.props.groups[groupName]['processes']), function (processName) {
					var process_dict =  that.props.groups[groupName]['processes'][processName];
					return (
						<li>
							<div className="hidden">{processName} - {process_dict['avg_time_taken']}</div>
							<span className="fa fa-check-circle"></span>
							<span className="fa fa-exclamation-triangle flash"></span>
							<span className="fa fa-cog fa-spin"></span>
						</li>
						)
						})}
					</ul>
					<div className="status-text hidden">
						<span className="label">Status</span>
						<span>All good!</span>
					</div>
					<div className="status-text warning">
						<span className="label">ID on the Intersection</span>
						<span className="text">Something is wrong!</span>
					</div>
					<div className="stats">
						<div className="col">
							<span className="label">Items in queue</span>
							300
						</div>
						<div className="col">
							<span className="label">Average per item</span>
							<span className="small">x&#772;:</span> 30s
						</div>
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
