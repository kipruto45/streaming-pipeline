import pytest
from processors.anomaly_detector import AnomalyDetector, AnomalyAlert

def test_anomaly_detector_no_alert_initially():
    detector = AnomalyDetector(window_size=10, threshold=3.0)
    for i in range(9):
        event = {'sensor_id': 's1', 'value': 50.0, 'timestamp': 1000 + i}
        assert detector.process(event) is None

def test_anomaly_detector_detects_spike():
    detector = AnomalyDetector(window_size=20, threshold=2.0)
    # Fill history with steady values
    for i in range(15):
        detector.process({'sensor_id': 's1', 'value': 50.0, 'timestamp': 1000 + i})
    
    # Send a spike
    spike_event = {'sensor_id': 's1', 'value': 100.0, 'timestamp': 2000}
    alert = detector.process(spike_event)
    
    assert alert is not None
    assert alert.sensor_id == 's1'
    assert alert.value == 100.0
    assert alert.z_score > 2.0
