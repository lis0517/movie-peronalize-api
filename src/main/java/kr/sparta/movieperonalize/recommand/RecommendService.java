package kr.sparta.movieperonalize.recommand;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import jakarta.annotation.PostConstruct;
import kr.sparta.movieperonalize.recommand.dto.MovieDto;
import kr.sparta.movieperonalize.recommand.enumtype.MovieGenre;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

@Slf4j
@Service
public class RecommendService {

	private static final String MOVIE_CACHE_KEY = "all_movies";

	private static final int CACHE_TTL_MINUTES = 60;

	private static final int RETRY_COUNT = 3;
	private static final int RETRY_OFFSET = 500;
	private static final int TIME_OUT = 3;

	@Value("${api.movie-info-api}")
	private String movieInfoApiUrl;
	private final WebClient.Builder webClientBuilder;

	private final RedisTemplate<String, Object> redisTemplate;
	private WebClient webClient;
	private UriComponents movieInfoApiUriComponent;

	public RecommendService(WebClient.Builder webClientBuilder, RedisTemplate<String, Object> redisTemplate) {
		this.webClientBuilder = webClientBuilder;
		this.redisTemplate = redisTemplate;
	}

	@PostConstruct
	private void init() {
		this.webClient = webClientBuilder.build();
		this.movieInfoApiUriComponent = UriComponentsBuilder.fromUriString(movieInfoApiUrl).path("/movies").build();
	}

	private Flux<MovieDto> getAllMovies() {
		List<MovieDto> cachedMovies = getCachedMovies();
		if (cachedMovies != null) {
			return Flux.fromIterable(cachedMovies);
		}

		return webClient.get()
			.uri(movieInfoApiUriComponent.toUriString())
			.retrieve()
			.bodyToFlux(MovieDto.class)
			.collectList()
			.doOnNext(movies -> {
				redisTemplate.opsForValue().set(MOVIE_CACHE_KEY, movies, Duration.ofMinutes(CACHE_TTL_MINUTES));
			})
			.flatMapMany(Flux::fromIterable)
			.retryWhen(Retry.backoff(RETRY_COUNT, Duration.ofMinutes(RETRY_OFFSET)))
			.timeout(Duration.ofSeconds(TIME_OUT));
	}

	public Flux<MovieDto> getMoviesByGenre(MovieGenre movieGenre) {
		return getAllMovies().filter(movieDto -> movieDto.getGenre().contains(movieGenre.getKorean()));
	}

	private List<MovieDto> getCachedMovies() {
		Object cachedValue = redisTemplate.opsForValue().get(MOVIE_CACHE_KEY);
		if(cachedValue instanceof List){
			return ((List<?>)cachedValue).stream()
				.filter(item -> item instanceof MovieDto)
				.map(item -> (MovieDto)item)
				.collect(Collectors.toList());
		}
		return null;
	}
}