import math

class GeoClusterManager:
    """
    Manages geographical distance calculations and cluster generation
    using the Haversine formula and graph-based Connected Components algorithm.
    """



    @staticmethod
    def _haversine(lat1, lon1, lat2, lon2):
        """Calculates the great-circle distance in kilometers between two geographic points."""
        R = 6371.0  # Earth radius in km
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
        return R * 2 * math.asin(math.sqrt(a))

    @classmethod
    def calculate_dynamic_threshold(cls, valid_items):
        """Computes a dynamic distance threshold based on actual point-to-point distances."""
        n = len(valid_items)
        if n <= 1:
            return 100.0 
        
        total_dist = 0
        pairs_count = 0
        for i in range(min(n, 20)): 
            for j in range(i + 1, min(n, 20)):
                _, _, lat1, lon1 = valid_items[i]
                _, _, lat2, lon2 = valid_items[j]
                total_dist += cls._haversine(lat1, lon1, lat2, lon2)
                pairs_count += 1
                
        if pairs_count == 0:
            return 500.0
            
        avg_dist = total_dist / pairs_count
        return max(avg_dist * 0.5, 50.0)

    @classmethod
    def generate_clusters(cls, data_dict):
        """Groups nearby geographic points into clusters and computes mean and median coordinates."""
        if not data_dict:
            return {}

        valid_items = []
        for record_id, record in data_dict.items():
            lat = record.get("latitude")
            lon = record.get("longitude")
            if lat is not None and lon is not None:
                valid_items.append((record_id, record, float(lat), float(lon)))

        if not valid_items:
            return {}

        threshold_km = cls.calculate_dynamic_threshold(valid_items)

        n = len(valid_items)
        adj = {i: [] for i in range(n)}
        for i in range(n):
            for j in range(i + 1, n):
                _, _, lat1, lon1 = valid_items[i]
                _, _, lat2, lon2 = valid_items[j]
                if cls._haversine(lat1, lon1, lat2, lon2) <= threshold_km:
                    adj[i].append(j)
                    adj[j].append(i)

        visited = [False] * n
        clusters = []

        for i in range(n):
            if not visited[i]:
                component = []
                queue = [i]
                visited[i] = True
                while queue:
                    curr = queue.pop(0)
                    component.append(curr)
                    for neighbor in adj[curr]:
                        if not visited[neighbor]:
                            visited[neighbor] = True
                            queue.append(neighbor)
                clusters.append(component)

        result_clusters = {}
        for idx, comp in enumerate(clusters):
            cluster_id = f"cluster_id{idx + 1}"
            lats = [valid_items[i][2] for i in comp]
            lons = [valid_items[i][3] for i in comp]

            mean_lat = sum(lats) / len(lats)
            mean_lon = sum(lons) / len(lons)

            sorted_lats = sorted(lats)
            sorted_lons = sorted(lons)
            mid = len(comp) // 2
            if len(comp) % 2 == 0:
                median_lat = (sorted_lats[mid - 1] + sorted_lats[mid]) / 2
                median_lon = (sorted_lons[mid - 1] + sorted_lons[mid]) / 2
            else:
                median_lat = sorted_lats[mid]
                median_lon = sorted_lons[mid]

            cluster_obj = {
                "median_coordinates": [median_lon, median_lat],
                "mean_coordinates": [mean_lon, mean_lat],
                "dynamic_threshold_used": threshold_km  
            }

            for i in comp:
                record_id, record, _, _ = valid_items[i]
                cluster_obj[record_id] = record

            result_clusters[cluster_id] = cluster_obj

        return result_clusters