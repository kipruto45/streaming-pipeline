import pytest
from processors.anomaly_detector import AnomalyDetector

def test_anomaly_detector_window_sliding():
    # Test that window doesn't grow indefinitely
    window_size = 5
    detector = AnomalyDetector(window_size=window_size)
    
    for i in range(10):
        detector.process({'sensor_id': 's1', 'value': float(i), 'timestamp': i})
    
    assert len(detector.history['s1']) == window_size
    # Last 5 values should be 5, 6, 7, 8, 9
    assert detector.history['s1'] == [5.0, 6.0, 7.0, 8.0, 9.0]

def test_anomaly_detector_min_samples():
    # Test that it needs at least 10 samples for Z-score (per logic in implementation)
    detector = AnomalyDetector()
    for i in range(9):
        res = detector.process({'sensor_id': 's1', 'value': 50.0, 'timestamp': i})
        assert res is None
    
    # 10th sample should still be None if no anomaly
    res = detector.process({'sensor_id': 's1', 'value': 50.0, 'timestamp': 9})
    assert res is None
